
## zookeeper znode架构
- BrokerNode：存储服务器的元数据，包括服务器的 name、url、port、负载情况等
- BunbleNode：存储 bundle 的元数据，包括 id、负责哈希环中某部分的收尾边界、所属 broker 的 url 等
- TopicNode：存储主题的元数据，包括主题的 name、分区数量、发布模式
- PartirionNode：存储分区的元数据，包括分区的 id、所属 topic name、已确认消息的编号、已推送消息的编号等
- SubscriptionNode：存储订阅的元数据，包括订阅的 name、所属 topic name、订阅分区的编号、订阅类型
- SuberNode：消费者的 id、订阅的 topic name、订阅的分区id 等
- PuberNode：生产者的 id、发布的 topic name、发布的分区id 等
- LeaderNode：leader broker 的 url



## 生产模式
- 1.独占：通过 zookeeper 在每个 partition 下生成一个 leader 节点，当生产者连接到 broker 后，检查该 partition 下是否存在 leader 节点，不存在即成为 leader 正常发布，存在则返回生产失败。注意使用临时节点注册 znode 以防止 leader 节点宕机异常退出无法释放 znode
- 2.备灾：跟独占模式类似，检查到存在 leader 节点时通过 zookeeper watch 机制阻塞直到 leader 节点退出
- 3.共享：无额外限制和操作，不生成 leader 节点

## 订阅模式
- 1.独占：通过 zookeeper 在每个 subscription 下生产一个leader 节点，当消费者连接到 subscription 后，检查该 subscription 下是否存在 leader 节点，不存在即成为 leader 正常消费，存在则返回订阅失败。注意使用 znode 以防止 leader 节点宕机异常退出无法释放 znode
- 2.备灾：跟独占模式类似，检查到存在 leader 节点时通过 zookeeper watch 机制阻塞直到 leader 节点退出
- 3.共享：无额外限制和操作，不生成 leader 节点

## 消费模式
- push
- pull
- Dynamic Pull

## 负载均衡
