package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, fmt.Errorf("raw get err: %s", err)
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, fmt.Errorf("raw get err: %s", err)
	}

	resp := &kvrpcpb.RawGetResponse{
		Value:    val,
		NotFound: val == nil,
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, fmt.Errorf("raw put err: %s", err)
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modify := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		return nil, fmt.Errorf("raw delete err: %s", err)
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, fmt.Errorf("raw scan err: %s", err)
	}
	limit := req.Limit
	startKey := req.StartKey
	pairs := make([]*kvrpcpb.KvPair, 0)
	var i uint32 = 0

	it := reader.IterCF(req.Cf)
	for it.Seek(startKey); it.Valid(); it.Next() {
		if i >= limit {
			break
		}
		i++
		item := it.Item()

		val, err := item.Value()
		if err != nil {
			return nil, fmt.Errorf("scan item err: %s", err)
		}
		pair := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		}
		pairs = append(pairs, pair)
	}

	resp := &kvrpcpb.RawScanResponse{
		Kvs: pairs,
	}
	return resp, nil
}
