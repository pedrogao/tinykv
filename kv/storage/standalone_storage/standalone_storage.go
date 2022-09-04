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
	db     *badger.DB
	dbPath string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		dbPath: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = s.dbPath
	opts.ValueDir = s.dbPath
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("open badger db err: %s", err)
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// a read-only transaction
	// And you don’t need to consider the kvrpcpb.Context now, it’s used in the following projects.
	txn := s.db.NewTransaction(false)
	return newBadgerReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// And you don’t need to consider the kvrpcpb.Context now, it’s used in the following projects.
	err := s.db.Update(func(txn *badger.Txn) error {
		var err error
		for _, modify := range batch {
			switch modify.Data.(type) {
			case storage.Put:
				err = txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
			case storage.Delete:
				err = txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
			}
			if err != nil {
				return fmt.Errorf("write badger modify: %v err: %s", modify, err)
			}
		}
		return nil
	})
	return err
}
