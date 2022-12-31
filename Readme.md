# Xmq

Xmq 是一个运行于 Linux 平台下的分布式发布-订阅消息平台，并结合了 Pulsar 以及传统消息系统的最佳功能。


# 主要特点

- 统一消费模型，支持队列消费，也支持标准发布订阅模式
- 灵活的发布和消费方式，支持备灾模式等
- 支持分区主题，增加系统吞吐量，提高单个主题的处理速度
- 采用存算分离设计，元数据保存在 zookeeper，数据保存在 etcd
- broker 实现无状态，上下线轻量，后期可达到无损水平扩展
- 基于 Dynamic Push/Pull 模型，优化消费端性能
- 实现一致性哈希+根据节点状态做细粒度负载均衡
- 抽象 bundle 层，减少 topic 迁移，优化负载均衡性能

# Xmq 使用说明

### 前提需要，请确保安装并打开：

建议版本：
- go 1.17
- zookeeper 3.4.13
- etcd 3.2.26

### 构建及使用

1.编译生成可执行文件
```
go build
```

2.运行，需指定参数以及配置文件
```
./Xmq start -c ./config/config.yaml
```

**注意**：运行前可修改配置文件,相关说明见[配置说明](https://github.com/bxiiiiii/Xmq/blob/master/config/config.go)

# 客户端使用说明详见具体实现

Go：[Publisher/Subscriber](https://github.com/bxiiiiii/Xmq-client-go/blob/master/Readme.md)

# 客户端
- [Go Client](https://github.com/bxiiiiii/Xmq-client-go)
