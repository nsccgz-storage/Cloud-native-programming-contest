# 针对冷热读写场景的RocketMQ存储系统设计

## 目前成果

目前只实现了把数据持久化到 PMEM 和从 PMEM 读数据的过程。
主要设计了如下的存储方式：
- TopicId: 考虑到题目要求 topicId 的数量 <= 100，所以直接在 PMEM 分配固定大小为 100 的 TopicArray， TopicArray 的每个元素存的是 `QueueId指针, topicName`。
    - QueueId：考虑到 queueId 的数量 <= 10000，所以直接在 PMEM 分配固定大小为 10000 的 QueueIdArray，QueueIdArray 的每个元素存的是 `Data指针, queueName`。
        - Data：采用链式存储，`data, Data指针`
## TODO
- 需要再实现同步到 SSD 上的过程
- 要利用 DRAM 来缓存部分数据
- 取多个数据目前还存在 bug 
- 考虑并发
