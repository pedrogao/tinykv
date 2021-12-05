package standalone_storage

import (
	"fmt"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf, db: nil}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.conf.DBPath
	opts.ValueDir = s.conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("open badger err:%s", err)
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	return err
}

// Reader should return a StorageReader that supports key/value's point get and scan operations on a snapshot.
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// you don’t need to consider the kvrpcpb.Context now, it’s used in the following projects.
	txn := s.db.NewTransaction(false)
	return NewSnapshotReader(txn), nil
}

// Write should provide a way that applies a series of modifications to the inner state which is, in this situation, a badger instance.
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// you don’t need to consider the kvrpcpb.Context now, it’s used in the following projects.
	txn := s.db.NewTransaction(true)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
			if err != nil {
				txn.Discard()
				return err
			}
		case storage.Delete:
			del := m.Data.(storage.Delete)
			err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
			if err != nil {
				txn.Discard()
				return err
			}
		}
	}
	err := txn.Commit()
	return err
}
