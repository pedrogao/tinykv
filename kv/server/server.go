package server

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	// 1. get lock
	// keys := [][]byte{req.Key}
	resp := &kvrpcpb.GetResponse{}
	ts := req.Version

	// 2. read
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return nil, err
	}

	// the key is locked by someone
	// but lock is not committed, so can't see
	// if the lock is committed, then the key is visible
	if lock != nil && lock.Ts < ts {
		resp.Error = &kvrpcpb.KeyError{
			Locked: lock.Info(req.Key),
		}
		return resp, nil
	}

	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		resp.NotFound = true
		return resp, nil
	}

	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}
	startTs := req.StartVersion
	lockTtl := req.LockTtl
	pk := req.PrimaryLock
	// 1. get lock
	keys := [][]byte{}
	for _, m := range req.Mutations {
		keys = append(keys, m.Key)
	}
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	// 2. read
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, startTs)
	// Prewrite all mutations in the request.
	for _, m := range req.Mutations {
		keyError, err := server.prewriteMutation(txn, m, startTs, lockTtl, pk)
		if keyError != nil {
			resp.Errors = append(resp.Errors, keyError)
		} else if err != nil {
			return nil, err
		}
	}

	server.Latches.Validation(txn, keys)

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (server *Server) prewriteMutation(txn *mvcc.MvccTxn, mut *kvrpcpb.Mutation,
	ts, lockTtl uint64, pk []byte) (*kvrpcpb.KeyError, error) {
	var (
		key                  = mut.Key
		err                  error
		lock                 *mvcc.Lock
		currentWrite         *mvcc.Write
		currentWriteCommitTs uint64
	)
	log.Debugf("prewrite mutation: %v at %d", mut, ts)
	// Find the current write record.
	currentWrite, currentWriteCommitTs, err = txn.MostRecentWrite(key)
	if err != nil {
		return nil, err
	}
	// Check for conflict with lock.
	if currentWrite != nil && currentWriteCommitTs > ts {
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    ts,
				ConflictTs: currentWriteCommitTs,
				Key:        key,
			},
		}, nil
	}
	// Rollback already
	if currentWrite != nil && currentWriteCommitTs == ts && currentWrite.Kind == mvcc.WriteKindRollback {
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    ts,
				ConflictTs: currentWriteCommitTs,
				Key:        key,
			},
		}, nil

	}

	lock, err = txn.GetLock(key)
	if err != nil {
		return nil, fmt.Errorf("get lock for key %s failed: %s", hex.EncodeToString(key), err)
	}
	if lock != nil {
		// support retry & reentrant
		if lock.Ts == ts && lock.Kind.ToProto() == mut.Op {
			return nil, nil
		}

		return &kvrpcpb.KeyError{
			Locked: lock.Info(key),
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    ts,
				ConflictTs: lock.Ts,
				Key:        key,
				Primary:    lock.Primary,
			},
		}, nil
	}

	switch mut.Op {
	case kvrpcpb.Op_Put:
		txn.PutValue(key, mut.Value)
	case kvrpcpb.Op_Del:
		txn.DeleteValue(key)
	default:
		return nil, fmt.Errorf("unsupported mutation operation: %s", mut.Op)
	}

	lock = &mvcc.Lock{Primary: pk, Ts: ts, Ttl: lockTtl, Kind: mvcc.WriteKindFromProto(mut.Op)}
	txn.PutLock(key, lock)

	return nil, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	startTs := req.StartVersion
	commitTs := req.CommitVersion
	keys := req.Keys
	// 1. get lock
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	// 2. read
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, startTs)
	// Commit all keys in the request.
	for _, key := range keys {
		keyError, err := server.commitKey(key, startTs, commitTs, txn)
		if keyError != nil {
			resp.Error = keyError
		} else if err != nil {
			return nil, err
		}
	}

	server.Latches.Validation(txn, keys)

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (server *Server) commitKey(key []byte, start, commitTs uint64, txn *mvcc.MvccTxn) (*kvrpcpb.KeyError, error) {
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}

	log.Debugf("commit key: %v start at %d, commit at %d", key, start, commitTs)

	if lock == nil {
		write, _, err := txn.MostRecentWrite(key)
		if err != nil {
			return nil, err
		}

		if write == nil {
			return nil, nil
		}

		if write.Kind == mvcc.WriteKindRollback {
			keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("%v already rollback", key)}
			return keyError, nil
		}

		return nil, nil
	}

	if lock.Ts != start {
		keyError := &kvrpcpb.KeyError{Retryable: fmt.Sprintf("conflicts with others %v", key)}
		return keyError, nil
	}

	// Commit a Write object to the DB
	write := mvcc.Write{StartTS: start, Kind: lock.Kind}
	txn.PutWrite(key, commitTs, &write)
	// Unlock the key
	txn.DeleteLock(key)

	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	var pairs []*kvrpcpb.KvPair
	limit := int(req.Limit)
	for i := 0; i < limit; {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}

		if lock != nil && req.Version >= lock.Ts {
			pairs = append(pairs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: lock.Primary,
						LockVersion: lock.Ts,
						Key:         key,
						LockTtl:     lock.Ttl,
					}},
				Key: key,
			})
			i++
			continue
		}
		if value != nil {
			pairs = append(pairs, &kvrpcpb.KvPair{Key: key, Value: value})
			i++
		}
	}
	resp.Pairs = pairs
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	write, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}
	if write != nil {
		// Already rollbacked.
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = ts
		}
		return resp, nil
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if lock == nil {
		// Rollback
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}

		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}

	// Lock expired
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err = server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return nil, err
		}

		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	if len(req.Keys) == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	for _, key := range req.Keys {
		write, _, err := txn.CurrentWrite(key)
		if err != nil {
			return nil, err
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				resp.Error = &kvrpcpb.KeyError{Abort: fmt.Sprintf("%v already committed", key)}
				return resp, nil
			}
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil || lock.Ts != req.StartVersion {
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	if req.StartVersion == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}

	defer reader.Close()
	iter := reader.IterCF(engine_util.CfLock)
	if err != nil {
		return nil, err
	}

	defer iter.Close()
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		return resp, nil
	}

	if req.CommitVersion == 0 {
		resp1, err := server.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		resp.Error = resp1.Error
		resp.RegionError = resp1.RegionError
		return resp, err
	} else {
		resp1, err := server.KvCommit(ctx, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		resp.Error = resp1.Error
		resp.RegionError = resp1.RegionError
		return resp, err
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
