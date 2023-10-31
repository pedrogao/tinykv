# project3

> Th hard part of tinykv

## 实现 Raft 领导者变更、成员变更

**TransferLeader**实现步骤：

1. `Leader`收到`TransferLeader`消息后，检查`transferee`是否满足当 leader 条件，主要是判断日志是否为最新的，如果不是最新的，发送`MsgAppend`消息，让`transferee`更新日志。
2. `transferee`收到`MsgAppend`消息后，更新日志，满足 leader 条件后，`Leader`发送`MsgTimeoutNow`消息至`transferee`，让`transferee`发起新的选举，成为新的 leader。

**Confchange**实现步骤：

1. 不支持多个节点同时加入、删除，只支持一个个节点加入、删除（one by one）。
2. 如果在删除节点的同时，正在进行`Leader`变更，那么停止变更流程。

## 实现配置变更，分区分裂

RegionEpoch 是 metapb.Region 元信息的一部分。当一个 Region 添加或删除 Peer 或 split 时，该 Region 的 epoch 就会发生变化。 RegionEpoch 的 `conf_ver` 在 ConfChange 期间增加，而 version 在拆分期间增加。

保证一个 Region 中的两个领导者在网络隔离的情况下获得最新的 Region 信息。

## 解读调度器

负责管理集群，担任集群调度，节点变更等任务。

### 收集 Region 心跳

### Region 负载均衡调度

## 参考资料

- [Project3 MultiRaftKV](https://github.com/Smith-Cruise/TinyKV-White-Paper/blob/main/Project3-MultiRaftKV.md)
- [project3.md](https://github.com/sakura-ysy/TinyKV-2022-doc/blob/main/doc/project3.md)
