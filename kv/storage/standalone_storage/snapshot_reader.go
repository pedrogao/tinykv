package standalone_storage

import (
	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type SnapshotReader struct {
	txn *badger.Txn
}

func NewSnapshotReader(txn *badger.Txn) *SnapshotReader {
	return &SnapshotReader{txn: txn}
}

func (s *SnapshotReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *SnapshotReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s *SnapshotReader) Close() {
	s.txn.Discard()
}
