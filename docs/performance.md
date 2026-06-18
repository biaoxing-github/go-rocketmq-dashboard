# goadmin 命令性能对比

本表是公开版命令级对比摘要，来源于本仓库的本地 Docker/RocketMQ 真实对照记录。原始验证口径是：官方 `mqadmin` 作为基线，`goadmin` 分别走 sidecar/兼容路径和 Go 原生路径，比较 stdout、stderr、exit code 或经过说明的动态字段归一化结果。

- `0` 表示与官方输出完全一致。
- `0*` 表示只有 MsgId、时间戳、运行时 TPS、导出时间等动态字段做了归一化，业务内容与官方一致。
- `avg ms` 是同一行原始多次采样的算术平均值，用于看相对量级；真实耗时会随机器、容器、Broker 状态和 JVM 冷热状态波动。
- 表内仅保留公开展示需要的信息，省略内部验证日志、完整临时文件路径、测试样本名和冗长备注。

共收录 156 条命令/场景对比。

| 命令/场景 | 官方 `mqadmin` avg ms | sidecar avg ms | Go 原生 avg ms | diff(sidecar/native) |
| --- | ---: | ---: | ---: | --- |
| `topicList`<br>非 `-c` | 4312 | 20.8 | 7.4 | `0` / `0` |
| `topicList -c`<br>cluster model | 3825 | 2918 | 13.8 | `0` / `0` |
| `topicRoute`<br>默认 JSON | 5424 | 1439 | 426 | `0` / `0` |
| `topicRoute -l`<br>`<sample>` | 4067 | 3112 | 5.4 | `0` / `0` |
| `topicStatus`<br>`<sample>`；sidecar 补测 `<sample>` | 4154 | 1437 | 136 | `0` / `0` |
| `topicStatus -c`<br>`<sample> -c DefaultCluster` | 4465 | 26.2 | 7.4 | `0` / `0` |
| `topicClusterList`<br>`<sample>`；sidecar 补测 `<sample>` | 4199 | 1440 | 131 | `0` / `0` |
| `clusterList`<br>默认基础表 / `DefaultCluster` | 4195 | 49.8 | 19.2 | `0` / `0` |
| `clusterList -m`<br>moreStats / `DefaultCluster` | 3716 | 41.8 | 18.2 | `0` / `0` |
| `brokerStatus`<br>`-b <broker>` | 4991 | 1392 | 16.8 | `0` / `0` |
| `brokerStatus`<br>`-c DefaultCluster` | 4111 | 40.8 | 23.8 | `0` / `0` |
| `getBrokerConfig`<br>`-b <broker>` | 7205 | 25.8 | 6.8 | `0` / `0` |
| `getBrokerConfig`<br>`-c DefaultCluster` | 4207 | 32 | 8.2 | `0` / `0` |
| `getNamesrvConfig`<br>`-n <broker>` | 4103 | 14.8 | 5.6 | `0` / `0` |
| `getConsumerConfig`<br>`-g <consumer_group>` | 4804 | 1516 | 9 | `0` / `0` |
| `consumerProgress`<br>`-g <sample> -t <sample> -c DefaultCluster` | 3970 | 43.4 | 11 | `0` / `0` |
| `brokerConsumeStats`<br>`-b <broker> -t 50000` | 8009 | 4224 | 14.6 | `0` / `0` |
| `producerConnection`<br>`-g <consumer_group> -t <sample>` | 5439 | 39.6 | 11.6 | `0` / `0` |
| `queryCq`<br>`-t <sample> -q 0 -i 0 -c 5` | 4856 | 29 | 6.8 | `0` / `0` |
| `haStatus`<br>`-c DefaultCluster` | 4095 | 20.6 | 8.2 | `0` / `0` |
| `queryMsgByKey`<br>`-c DefaultCluster -m 1` | 3851 | 630 | 6.8 | `0` / `0` |
| `queryMsgByOffset`<br>queueId=`0` offset=`0` | 4884 | 52.4 | 15.2 | `0` / `0` |
| `queryMsgByOffset -f GBK`<br>raw GBK body=`中文` | 4061 | 5747 | 8 | `0` / `0` |
| `queryMsgById`<br>OffsetID | 4319 | 1766 | 6.6 | `0` / `0` |
| `queryMsgById`<br>`UNIQ_KEY` | 3893 | 30.8 | 8.8 | `0` / `0` |
| `queryMsgById -f GBK`<br>raw GBK body=`中文` | 4334 | 4303 | 10.4 | `0` / `0` |
| `queryMsgById -g -d`<br>`<consumer_group>` 临时 push consumer 直接消费 | 3871 | 30.4 | 16.4 | `0*` / `0*` |
| `queryMsgById -s true`<br>重发原 MsgId | 3736 | 26.8 | 10.6 | `0*` / `0*` |
| `queryMsgByUniqueKey`<br>`UNIQ_KEY` | 4302 | 1777 | 6.6 | `0` / `0` |
| `queryMsgByUniqueKey -a`<br>`UNIQ_KEY` showAll | 4143 | 36.6 | 6 | `0` / `0` |
| `queryMsgByUniqueKey -g -d`<br>`<consumer_group>` 在线 push consumer 直接消费 | 4362 | 4784 | 117 | `0` / `0` |
| `queryMsgTraceById`<br>key=`TRACE-<sample>` | 4230 | 25.2 | 9.4 | `0` / `0` |
| `consumerProgress`<br>`-g <sample> -t <sample>` | 4511 | 1437 | 13.2 | `0` / `0` |
| `consumerProgress`<br>无 `-g` 在线汇总样本 | 3929 | 4210 | 12.8 | `0` / `0` |
| `consumerProgress`<br>`-c DefaultCluster` | 4355 | 1464 | 10.4 | `0` / `0` |
| `consumerProgress`<br>`-g <consumer_group> -s true` | 4048 | 34.2 | 12.8 | `0` / `0` |
| `consumerConnection`<br>`-g <consumer_group>` | 3724 | 1358 | 6.6 | `0` / `0` |
| `consumerStatus`<br>`-g <sample>` 列表/文件模式 | 4366 | 1487 | 15.6 | `0*` / `0*` |
| `consumerStatus -i`<br>`-g <sample> -i <clientId>` | 4655 | 56 | 18.8 | `0*` / `0*` |
| `consumerStatus -i -b`<br>`-g <sample> -i <clientId> -b <broker>` | 3873 | 71.6 | 26 | `0*` / `0*` |
| `statsAll`<br>`-t <sample>` | 4594 | 146 | 129 | `0` / `0` |
| `allocateMQ`<br>`-t <sample> -i <ip>,<ip>` | 4206 | 4281 | 139 | `0` / `0` |
| `printMsgByQueue`<br>`-t <sample> -a <broker> -i 0 -b <timestamp> -e <timestamp> -p true -d false` | 4086 | 4565 | 133 | `0` / `0` |
| `printMsgByQueue -f`<br>`-t <sample> -a <broker> -i 0 -p false -f true` | 3985 | 46.2 | 10.8 | `0` / `0` |
| `printMsg`<br>`-t <sample> -b <timestamp> -e <timestamp> -d false` | 4074 | 49.4 | 19.2 | `0` / `0` |
| `producer`<br>`-b <broker>` | 4370 | 21.2 | 5.4 | `0*` / `0` |
| `consumeMessage`<br>`-t <sample> -b <broker> -i 0 -o 0 -c 1` | 4436 | 173 | 9.2 | `0` / `0` |
| `getColdDataFlowCtrInfo`<br>`-b <broker>`；`-c DefaultCluster` 也已对照 | 4955 | 1423 | 5 | `0` / `0` |
| `exportConfigs`<br>`-c DefaultCluster -f /tmp/<sample>` | 4213 | 27.6 | 9.6 | `0` / `0` |
| `exportMetadata`<br>`-c DefaultCluster -f /tmp/<sample>`；`-b -t/-g` 也已对照 | 3891 | 2776 | 8.4 | `0` / `0` |
| `exportMetrics`<br>`-c DefaultCluster -f /tmp/<sample>` | 4294 | 43.8 | 21.8 | `0` / `0` |
| `checkRocksdbCqWriteProgress`<br>`-c DefaultCluster -t <sample> -cf 0` | 4144 | 1510 | 6 | `0` / `0` |
| `rocksDBConfigToJson`<br>`-c DefaultCluster -t topics`；`-b` 与默认 `-t` 也已对照 | 3963 | 34.4 | 11.2 | `0` / `0` |
| `exportPopRecord`<br>`-c DefaultCluster -d false`；`-b` dry-run 与默认 actual 也已对照 | 3858 | 22 | 5 | `0` / `0` |
| `updateKvConfig`<br>`-s <sample> -k <temp> -v value` | 4858 | 19.2 | 6 | `0` / `0` |
| `deleteKvConfig`<br>`-s <sample> -k <temp>` | 4759 | 1821 | 6.8 | `0` / `0` |
| `updateTopic`<br>cluster/broker 临时 Topic | 4257 | 30.2 | 9.8 | `0` / `0` |
| `deleteTopic`<br>cluster 临时 Topic 清理 | 4079 | 26.8 | 12 | `0` / `0` |
| `updateSubGroup`<br>`-c DefaultCluster` 临时订阅组；`-b` 也已对照 | 4351 | 19.4 | 7.8 | `0` / `0` |
| `deleteSubGroup`<br>`-c DefaultCluster` 临时订阅组；`-b` 也已对照 | 4070 | 34.4 | 11.2 | `0` / `0` |
| `updateOrderConf put`<br>`-m put -t <sample> -v <broker>:1` | 4393 | 1595 | 5 | `0` / `0` |
| `updateOrderConf get`<br>`-m get -t <sample>` | 4257 | 11.8 | 6 | `0` / `0` |
| `updateOrderConf delete`<br>`-m delete -t <sample>` | 3777 | 12 | 4.6 | `0` / `0` |
| `updateBrokerConfig`<br>`-b <broker> -k enableDetailStat -v true`；`-c DefaultCluster` 也已对照 | 4590 | 1861 | 5.6 | `0` / `0` |
| `updateNamesrvConfig`<br>`-n <broker> -k clusterTest -v false` | 4856 | 18.2 | 11 | `0` / `0` |
| `updateTopicPerm`<br>`-c DefaultCluster -t <temp> -p 4`；`-b <route-master>`, same-perm 与非 master 错误也已对照 | 3742 | 20.6 | 7 | `0` / `0` |
| `setConsumeMode`<br>`-c DefaultCluster -t <sample> -g <sample> -m POP -q 1`；`-b <broker>` 也已对照 | 4693 | 22 | 8.6 | `0` / `0` |
| `updateColdDataFlowCtrGroupConfig`<br>`-c DefaultCluster -g <sample> -v <threshold>`；`-b <broker>` 也已对照 | 4803 | 16 | 7.6 | `0` / `0` |
| `removeColdDataFlowCtrGroupConfig`<br>`-c DefaultCluster -g <sample>`；`-b <broker>` 也已对照 | 3708 | 1353 | 8.2 | `0` / `0` |
| `updateTopicList`<br>`-c DefaultCluster -f <TopicConfig JSON>`；`-b <broker>` 也已对照 | 4048 | 2674 | 16.6 | `0` / `0` |
| `updateSubGroupList`<br>`-c DefaultCluster -f <SubscriptionGroupConfig JSON>`；`-b <broker>` 也已对照 | 3772 | 19.8 | 8.8 | `0` / `0` |
| `cleanExpiredCQ`<br>`-c DefaultCluster`；`-b <broker>` 也已对照 | 4604 | 1789 | 11.6 | `0` / `0` |
| `cleanUnusedTopic`<br>`-c DefaultCluster`；`-b <broker>` 也已对照 | 4738 | 34 | 9 | `0` / `0` |
| `deleteExpiredCommitLog`<br>`-c DefaultCluster`；`-b <broker>` 也已对照 | 4021 | 1385 | 8 | `0` / `0` |
| `wipeWritePerm`<br>`-n <broker> -b <broker>` | 4923 | 23.4 | 7.4 | `0` / `0` |
| `addWritePerm`<br>`-n <broker> -b <broker>` | 3840 | 22.8 | 7.6 | `0` / `0` |
| `cloneGroupOffset`<br>`-s <consumer_group> -d <tempGroup> -t <sample> -o true` | 3775 | 1450 | 11.6 | `0` / `0` |
| `cloneGroupOffset -o true`<br>`-s <consumer_group> -d <sample> -t %RETRY%<consumer_group> -o true` | 3854 | 35.6 | 9.2 | `0` / `0` |
| `cloneGroupOffset -o`<br>missing offline value parser preflight | 431 | 4.6 | 3.4 | `0` / `0` |
| `sendMessage`<br>`-t <sample> -b <broker> -i 0` | 3723 | 34.4 | 7.8 | `0*` / `0*` |
| `sendMessage -m true`<br>`-t <sample> -b <broker> -i 0 -m true` | 4173 | 158 | 134 | `0*` / `0*` |
| `sendMsgStatus`<br>`-b <broker> -s 16 -c 1` | 4501 | 1392 | 8.8 | `0*` / `0*` |
| `checkMsgSendRT`<br>`-t <sample> -s 16 -a 2` | 4580 | 35 | 11.8 | `0*` / `0*` |
| `resetMasterFlushOffset`<br>`-b <broker> -o 0` | 4739 | 3331 | 6.6 | `0` / `0` |
| `resetOffsetByTime`<br>`-b <broker> -q 0 -o 1 -s <timestamp>` | 4247 | 16 | 5.6 | `0` / `0` |
| `resetOffsetByTime`<br>`-g <tempGroup> -t <sample> -s -1 -f true` | 4491 | 1369 | 7.6 | `0` / `0` |
| `resetOffsetByTime`<br>`-b <broker> -q 0 -s 9999999999999` | 3746 | 19.2 | 8 | `0` / `0` |
| `resetOffsetByTime`<br>`-b <broker> -s -1` | 4463 | 4548 | 14 | `0` / `0` |
| `skipAccumulatedMessage`<br>`-g <tempGroup> -t <sample>` | 5232 | 27 | 7.4 | `0` / `0` |
| `updateStaticTopic`<br>`-b <broker> -qn 4 -t <tempTopic>` | 4355 | 44 | 10.4 | `0` / `0` |
| `updateStaticTopic -mf`<br>`-b <broker> -qn 4 -mf /tmp/<sample>` | 4805 | 19.6 | 9.2 | `0*` / `0*` |
| `remappingStaticTopic`<br>`-b <broker> -t <tempStaticTopic>` | 4284 | 3242 | 8.8 | `0` / `0` |
| `remappingStaticTopic -mf`<br>`-b <broker> -mf /tmp/<sample>` | 4507 | 1767 | 10.8 | `0*` / `0*` |
| `clusterRT`<br>`-a 2 -s 16 -i 1` | 2066 | 1033 | 1074 | `0` / `0` |
| `getControllerMetaData`<br>`-a 127.0.0.1:<port>` | 7185 | 15 | 6 | `0` / `0` |
| `getControllerConfig`<br>`-a 127.0.0.1:<port>` | 7260 | 40.8 | 12.8 | `0` / `0` |
| `getSyncStateSet`<br>`-a 127.0.0.1:<port> -b broker-a` | 7265 | 18.2 | 7.4 | `0` / `0` |
| `dumpCompactionLog`<br>`-f /tmp/<sample>` | 440 | 11.2 | 4 | `0` / `0` |
| `exportMetadataInRocksDB`<br>`-p /tmp/<sample> -t topics` | 417 | 6.2 | 3.4 | `0` / `0` |
| `exportMetadataInRocksDB`<br>`-p /tmp/<sample> -t topics` | 981 | 32.8 | 11 | `0` / `0` |
| `exportMetadataInRocksDB -j true`<br>`-p /tmp/<sample> -t subscriptionGroups -j true` | 1028 | 23.8 | 9.4 | `0` / `0` |
| `rocksDBConfigToJson`<br>`-p /tmp/<sample> -t topics` | 666 | 13.6 | 3.2 | `0` / `0` |
| `rocksDBConfigToJson -j false`<br>`-p /tmp/<sample> -t subscriptionGroups -j false` | 665 | 12.4 | 3.2 | `0` / `0` |
| `rocksDBConfigToJson -e`<br>`-p /tmp/<sample> -t topics -j false -e <file>` | 674 | 14.4 | 3.6 | `0` / `0` |
| `rocksDBConfigToJson`<br>`-p /tmp/<sample> -t consumerOffsets` | 927 | 18.2 | 5.4 | `0` / `0` |
| `rocksDBConfigToJson -j false`<br>`-p /tmp/<sample> -t consumerOffsets -j false` | 1021 | 20.2 | 5.8 | `0` / `0` |
| `rocksDBConfigToJson -e`<br>`-p /tmp/<sample> -t consumerOffsets -j false -e <file>` | 1044 | 32.6 | 5 | `0` / `0` |
| `updateControllerConfig`<br>`-a 127.0.0.1:<port> -k controllerDLegerGroup -v group1` | 7434 | 28 | 6.8 | `0` / `0` |
| `removeBroker`<br>`-c 127.0.0.1:<port> -b DefaultCluster:broker-a:-1` | 9140 | 9393 | 3 | `0` / `0` |
| `removeBroker`<br>`-c 127.0.0.1:<port> -b DefaultCluster:<sample>:0 --timeout-ms 60000` | 13264 | 8536 | 8529 | `0` / `0` |
| `addBroker`<br>`-c 127.0.0.1:<port> -b /tmp/<sample>*.conf` | 7948 | 123 | 103 | `0` / `0` |
| `getBrokerEpoch`<br>`-n <broker> -b <sample>` | 4255 | 20.8 | 6.2 | `0` / `0` |
| `getBrokerEpoch -c`<br>`-n <broker> -c <sample>` | 4133 | 20 | 6.4 | `0` / `0` |
| `cleanBrokerMetadata`<br>`-a 127.0.0.1:<port> -c <sample> -bn <sample> -b 0` | 6522 | 23.8 | 11.4 | `0` / `0` |
| `electMaster`<br>`-a 127.0.0.1:<port> -c <sample> -bn <sample> -b 3` | 8484 | 19.6 | 10.2 | `0*` / `0*` |
| `getSyncStateSet -c`<br>`-n <broker> -a 127.0.0.1:<port> -c <sample>` | 4519 | 1447 | 14.6 | `0` / `0` |
| `listUser`<br>`-b 127.0.0.1:<port> -f <sample>` | 8024 | 3119 | 6.4 | `0` / `0` |
| `listUser -c`<br>`-n <broker> -c <sample> -f <sample>` | 4418 | 19.8 | 7 | `0` / `0` |
| `getUser`<br>`-b 127.0.0.1:<port> -u <sample>` | 7429 | 29 | 6.2 | `0` / `0` |
| `getUser -c`<br>`-n <broker> -c <sample> -u <sample>` | 4441 | 23.8 | 7.4 | `0` / `0` |
| `createUser`<br>`-b 127.0.0.1:<port> -u <sample> -p <secret> -t Super` | 6847 | 23 | 7 | `0` / `0` |
| `createUser -c`<br>`-n <broker> -c <sample> -u <sample> -p <secret> -t Super` | 3809 | 25.6 | 8.6 | `0` / `0` |
| `updateUser`<br>`-b 127.0.0.1:<port> -u <sample> -s disable` | 6965 | 1776 | 7.6 | `0` / `0` |
| `updateUser -c`<br>`-n <broker> -c <sample> -u <sample> -s disable` | 3895 | 23.8 | 9.4 | `0` / `0` |
| `deleteUser`<br>`-b 127.0.0.1:<port> -u <sample>` | 6833 | 1447 | 11.2 | `0` / `0` |
| `deleteUser -c`<br>`-n <broker> -c <sample> -u <sample>` | 3789 | 23 | 12.8 | `0` / `0` |
| `setCommitLogReadAheadMode`<br>`-b 127.0.0.1:<port> -m 0` | 6691 | 14 | 5.8 | `0` / `0` |
| `setCommitLogReadAheadMode -c`<br>`-n <broker> -c <sample> -m 1` | 3753 | 1323 | 8 | `0` / `0` |
| `copyUser`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port> -u <sample>,<sample>` | 7026 | 1478 | 34.2 | `0` / `0` |
| `copyUser`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port>` | 7972 | 1491 | 39.6 | `0` / `0` |
| `createAcl`<br>`-b 127.0.0.1:<port> -s User:<sample> -r Topic:<sample> -a Pub,Sub -i <ip>,<ip> -d Allow` | 4275 | 18.6 | 8.8 | `0` / `0` |
| `createAcl -c`<br>`-n <broker> -c <sample> -s User:<sample> -r Topic:<sample> -a Pub,Sub -i <ip>,<ip> -d Allow` | 3926 | 26.6 | 10.4 | `0` / `0` |
| `updateAcl`<br>`-b 127.0.0.1:<port> -s User:<sample> -r Topic:<sample> -a Sub -d Deny` | 3894 | 17.2 | 6.4 | `0` / `0` |
| `updateAcl -c`<br>`-n <broker> -c <sample> -s User:<sample> -r Topic:<sample> -a Sub -d Deny` | 4506 | 35.8 | 16.2 | `0` / `0` |
| `deleteAcl`<br>`-b 127.0.0.1:<port> -s User:<sample> -r Topic:<sample>` | 4561 | 29.8 | 6.6 | `0` / `0` |
| `deleteAcl -c`<br>`-n <broker> -c <sample> -s User:<sample> -r Topic:<sample>` | 3977 | 19.4 | 8.4 | `0` / `0` |
| `getAcl`<br>`-b 127.0.0.1:<port> -s User:<sample>` | 4679 | 2724 | 5.8 | `0` / `0` |
| `getAcl -c`<br>`-n <broker> -c <sample> -s User:<sample>` | 3755 | 1370 | 7.6 | `0` / `0` |
| `listAcl`<br>`-b 127.0.0.1:<port> -s User:<sample>` | 3769 | 19.2 | 5.8 | `0` / `0` |
| `listAcl -c`<br>`-n <broker> -c <sample> -s User:<sample>` | 3789 | 18.8 | 9.6 | `0` / `0` |
| `copyAcl`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port> -s User:<sample>,User:<sample>` | 4188 | 1411 | 13 | `0` / `0` |
| `copyAcl update`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port> -s User:<sample>,User:<sample>` | 4196 | 35.8 | 12.8 | `0` / `0` |
| `copyAcl`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port>` | 3874 | 40.8 | 14.2 | `0` / `0` |
| `copyAcl update`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port>` | 3970 | 2840 | 15.6 | `0` / `0` |
| `updateAclConfig`<br>`-b 127.0.0.1:<port> -a <sample> -s <secret> -w <ip-pattern> -t PUB\\|SUB -g SUB -m true` | 10115 | 27.6 | 9.2 | `0` / `0` |
| `updateAclConfig -c`<br>`-n <broker> -c <sample> -a <sample> -s <secret> -w <ip-pattern> -t PUB\\|SUB -g SUB -m true` | 4718 | 29.4 | 12.6 | `0` / `0` |
| `deleteAclConfig`<br>`-b 127.0.0.1:<port> -a <sample>` | 12095 | 1404 | 8.4 | `0` / `0` |
| `deleteAclConfig -c`<br>`-n <broker> -c <sample> -a <sample>` | 4391 | 1406 | 9.6 | `0` / `0` |
| `deleteAccessConfig`<br>`-b 127.0.0.1:<port> -a <sample>` | 10343 | 1391 | 7.8 | `0` / `0` |
| `updateGlobalWhiteAddr`<br>`-b 127.0.0.1:<port> -g <ip-pattern> -p /tmp/<sample>` | 11006 | 18.4 | 7.4 | `0` / `0` |
| `updateGlobalWhiteAddr -c`<br>`-n <broker> -c <sample> -g <ip-pattern> -p /tmp/<sample>` | 4398 | 29.2 | 9.4 | `0` / `0` |
| `startMonitoring`<br>`-n <broker>` under external `timeout 2` | 2508 | 2145 | 2159 | `0` / `0` |
| `getBrokerEpoch -i`<br>`-n <broker> -c <sample> -i 1` | 2116 | 1270 | 1068 | `0` / `0` |
| `getSyncStateSet -i`<br>`-n <broker> -a 127.0.0.1:<port> -c <sample> -i 1` | 1774 | 15907 | 1066 | `0` / `0` |
| `haStatus -i`<br>`-b <broker> -i 1` | 5035 | 2824 | 1016 | `0` / `0` |
| `clusterAclConfigVersion`<br>`-n <broker> -c DefaultCluster`；`-b 127.0.0.1:<port>` ACL broker 也已实测 | 3722 | 24 | 2.8 | `0` / `0` |
