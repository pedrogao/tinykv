package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	ctx := &kvrpcpb.Context{}
	reader, err := server.storage.Reader(ctx)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		Value: val,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	ctx := &kvrpcpb.Context{}
	put := storage.Modify{Data: storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}}
	err := server.storage.Write(ctx, []storage.Modify{put})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{
		RegionError: nil,
		Error:       "",
	}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	ctx := &kvrpcpb.Context{}
	del := storage.Modify{Data: storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}}
	err := server.storage.Write(ctx, []storage.Modify{del})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{
		RegionError: nil,
		Error:       "",
	}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	ctx := &kvrpcpb.Context{}
	reader, err := server.storage.Reader(ctx)
	if err != nil {
		return nil, err
	}
	var (
		pairs  []*kvrpcpb.KvPair
		errRet error
		count  uint32 = 0
		val    []byte
	)
	iter := reader.IterCF(req.Cf)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if count >= req.Limit {
			break
		}
		item := iter.Item()
		val, errRet = item.Value()
		if errRet != nil {
			break
		}
		pair := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		}
		pairs = append(pairs, pair)
		count++
	}
	iter.Close()
	resp := &kvrpcpb.RawScanResponse{
		Kvs: pairs,
	}
	if errRet != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}
