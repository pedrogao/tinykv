# project4

## 4A

`MvccTxn`实现`Lock`，`Value`CRUD 操作。

## 4B

实现 Server 的 `KvGet`，`KvPrewrite`，`KvCommit` 三个方法。

注意 `Transaction` 的事务隔离级别。

## 4C

实现 Server 的 `KvScan`，`KvCheckTxnStatus`，`KvResolveLock`，`KvBatchRollback` 四个方法。

`KvScan`是最麻烦的一个方法，每个`Key`可能有多个版本，而遍历只需要拿到最新版本的`Value`，因此需要过滤掉旧版本的`Value`。
