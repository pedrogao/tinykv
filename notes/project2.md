# project2

## 架构设计导读

TinyKV 中使用了 Store、Peer 和 Region 的概念，Store 代表 tinykv 服务器实例，Peer 代表运行在 Store 上的 Raft 节点，Region 是一系列 Peers 的集合，也称为 Raft Group。在 Project2 中，只考虑每个 Store 上一个 Peer，总共一个 Region 的简单情况。

![server2](./imgs/server2.png)

一个`region`是多个`peer`的集合，每个`peer`都是一个 raft 节点，因此`region`也可以称为`raft group`。

每个`peer`上也有一个`storage`实例。

```mermaid
---
title: Rigion
---

graph TD;
  subgraph PeerA
    A[Leader A]
  end

  subgraph PeerB
    B[Follower B]
  end

  subgraph PeerC
    C[Follower C]
  end

  A -->|Heartbeat| B
  A -->|Heartbeat| C
  A -->|Log Replication| B
  A -->|Log Replication| C

```

分布式`Storage`在实现上非常复杂，不再直接将`KV`写入到磁盘中，而是先`KV`以`Raft Command`形式发送到其它节点，一致性协议达成一致后再写入到磁盘中。

层级架构比较深，不太好理解，如下：

![server](./imgs/server.png)

## 参考资料

- [谈谈 Raft 分布式共识性算法的实现](https://pedrogao.github.io/posts/distribute/raft.html#%E7%8A%B6%E6%80%81%E6%9C%BA)
- [project2-RaftKV](https://github.com/talent-plan/tinykv/blob/course/doc/project2-RaftKV.md)
- [TinyKV Project2 RaftKV](https://chenyunong.com/2021/08/04/TinyKV-Project2/)
- [Project2](https://www.charstal.com/talent-plan-tinykv-project2/)
- [TinyKV 实现笔记](http://blog.rinchannow.top/tinykv-notes/)
- [Talent Plan TinyKV 白皮书](https://www.inlighting.org/archives/talent-plan-tinykv-white-paper)
- [platoneko/tinykv](https://github.com/platoneko/tinykv)
- [raft zh_cn](https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md)
- [TinyKv Project2 PartA RaftKV](https://blog.csdn.net/qq_42956653/article/details/126954826)
- [TinyKV-2022-doc](https://github.com/sakura-ysy/TinyKV-2022-doc)
- [tinykv](https://github.com/platoneko/tinykv)
- [etcd Raft 库解析](https://www.codedump.info/post/20180922-etcd-raft/)
- [Talent Plan 之 TinyKV 学习营推荐课程](https://learn.pingcap.com/learner/course/390002)
- [可靠分布式系统-paxos 的直观解释](https://blog.openacid.com/algo/paxos/)
- [Talent Plan 2021 KV 学习营分享课](https://learn.pingcap.com/learner/player/510002)
