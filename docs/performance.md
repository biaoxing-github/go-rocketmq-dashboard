# goadmin 命令性能对比

本表是公开版命令级对比摘要，来源于本仓库的本地 Docker/RocketMQ 真实对照记录。原始验证口径是：官方 `mqadmin` 作为基线，`goadmin` 分别走 sidecar/兼容路径和 Go 原生路径，比较 stdout、stderr、exit code 或经过说明的动态字段归一化结果。

- `提升 = 提升倍数 / 耗时下降率`；提升倍数为 `官方 mqadmin 平均耗时 / 对应路径平均耗时`，例如 `100x / 99.0%` 表示平均耗时约为官方进程路径的 1/100。
- `0` 表示与官方输出完全一致。
- `0*` 表示只有 MsgId、时间戳、运行时 TPS、导出时间等动态字段做了归一化，业务内容与官方一致。
- `avg ms` 是同一行原始多次采样的算术平均值，用于看相对量级；真实耗时会随机器、容器、Broker 状态和 JVM 冷热状态波动。
- 表内仅保留公开展示需要的信息，省略内部验证日志、完整临时文件路径、测试样本名和冗长备注。

共收录 156 条命令/场景对比。

| 命令/场景 | 官方 `mqadmin` avg ms | sidecar avg ms | sidecar 提升 | Go 原生 avg ms | Go 原生提升 | diff(sidecar/native) |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `topicList`<br>非 `-c` | 4312 | 20.8 | 207x / 99.5% | 7.4 | 583x / 99.8% | `0` / `0` |
| `topicList -c`<br>cluster model | 3825 | 2918 | 1.31x / 23.7% | 13.8 | 277x / 99.6% | `0` / `0` |
| `topicRoute`<br>默认 JSON | 5424 | 1439 | 3.77x / 73.5% | 426 | 12.7x / 92.1% | `0` / `0` |
| `topicRoute -l`<br>`<sample>` | 4067 | 3112 | 1.31x / 23.5% | 5.4 | 753x / 99.9% | `0` / `0` |
| `topicStatus`<br>`<sample>`；sidecar 补测 `<sample>` | 4154 | 1437 | 2.89x / 65.4% | 136 | 30.5x / 96.7% | `0` / `0` |
| `topicStatus -c`<br>`<sample> -c DefaultCluster` | 4465 | 26.2 | 170x / 99.4% | 7.4 | 603x / 99.8% | `0` / `0` |
| `topicClusterList`<br>`<sample>`；sidecar 补测 `<sample>` | 4199 | 1440 | 2.92x / 65.7% | 131 | 32.1x / 96.9% | `0` / `0` |
| `clusterList`<br>默认基础表 / `DefaultCluster` | 4195 | 49.8 | 84.2x / 98.8% | 19.2 | 218x / 99.5% | `0` / `0` |
| `clusterList -m`<br>moreStats / `DefaultCluster` | 3716 | 41.8 | 88.9x / 98.9% | 18.2 | 204x / 99.5% | `0` / `0` |
| `brokerStatus`<br>`-b <broker>` | 4991 | 1392 | 3.59x / 72.1% | 16.8 | 297x / 99.7% | `0` / `0` |
| `brokerStatus`<br>`-c DefaultCluster` | 4111 | 40.8 | 101x / 99% | 23.8 | 173x / 99.4% | `0` / `0` |
| `getBrokerConfig`<br>`-b <broker>` | 7205 | 25.8 | 279x / 99.6% | 6.8 | 1060x / 99.9% | `0` / `0` |
| `getBrokerConfig`<br>`-c DefaultCluster` | 4207 | 32 | 131x / 99.2% | 8.2 | 513x / 99.8% | `0` / `0` |
| `getNamesrvConfig`<br>`-n <broker>` | 4103 | 14.8 | 277x / 99.6% | 5.6 | 733x / 99.9% | `0` / `0` |
| `getConsumerConfig`<br>`-g <consumer_group>` | 4804 | 1516 | 3.17x / 68.4% | 9 | 534x / 99.8% | `0` / `0` |
| `consumerProgress`<br>`-g <sample> -t <sample> -c DefaultCluster` | 3970 | 43.4 | 91.5x / 98.9% | 11 | 361x / 99.7% | `0` / `0` |
| `brokerConsumeStats`<br>`-b <broker> -t 50000` | 8009 | 4224 | 1.9x / 47.3% | 14.6 | 549x / 99.8% | `0` / `0` |
| `producerConnection`<br>`-g <consumer_group> -t <sample>` | 5439 | 39.6 | 137x / 99.3% | 11.6 | 469x / 99.8% | `0` / `0` |
| `queryCq`<br>`-t <sample> -q 0 -i 0 -c 5` | 4856 | 29 | 167x / 99.4% | 6.8 | 714x / 99.9% | `0` / `0` |
| `haStatus`<br>`-c DefaultCluster` | 4095 | 20.6 | 199x / 99.5% | 8.2 | 499x / 99.8% | `0` / `0` |
| `queryMsgByKey`<br>`-c DefaultCluster -m 1` | 3851 | 630 | 6.11x / 83.6% | 6.8 | 566x / 99.8% | `0` / `0` |
| `queryMsgByOffset`<br>queueId=`0` offset=`0` | 4884 | 52.4 | 93.2x / 98.9% | 15.2 | 321x / 99.7% | `0` / `0` |
| `queryMsgByOffset -f GBK`<br>raw GBK body=`中文` | 4061 | 5747 | 0.71x / -41.5% | 8 | 508x / 99.8% | `0` / `0` |
| `queryMsgById`<br>OffsetID | 4319 | 1766 | 2.45x / 59.1% | 6.6 | 654x / 99.8% | `0` / `0` |
| `queryMsgById`<br>`UNIQ_KEY` | 3893 | 30.8 | 126x / 99.2% | 8.8 | 442x / 99.8% | `0` / `0` |
| `queryMsgById -f GBK`<br>raw GBK body=`中文` | 4334 | 4303 | 1.01x / 0.7% | 10.4 | 417x / 99.8% | `0` / `0` |
| `queryMsgById -g -d`<br>`<consumer_group>` 临时 push consumer 直接消费 | 3871 | 30.4 | 127x / 99.2% | 16.4 | 236x / 99.6% | `0*` / `0*` |
| `queryMsgById -s true`<br>重发原 MsgId | 3736 | 26.8 | 139x / 99.3% | 10.6 | 352x / 99.7% | `0*` / `0*` |
| `queryMsgByUniqueKey`<br>`UNIQ_KEY` | 4302 | 1777 | 2.42x / 58.7% | 6.6 | 652x / 99.8% | `0` / `0` |
| `queryMsgByUniqueKey -a`<br>`UNIQ_KEY` showAll | 4143 | 36.6 | 113x / 99.1% | 6 | 690x / 99.9% | `0` / `0` |
| `queryMsgByUniqueKey -g -d`<br>`<consumer_group>` 在线 push consumer 直接消费 | 4362 | 4784 | 0.91x / -9.7% | 117 | 37.3x / 97.3% | `0` / `0` |
| `queryMsgTraceById`<br>key=`TRACE-<sample>` | 4230 | 25.2 | 168x / 99.4% | 9.4 | 450x / 99.8% | `0` / `0` |
| `consumerProgress`<br>`-g <sample> -t <sample>` | 4511 | 1437 | 3.14x / 68.1% | 13.2 | 342x / 99.7% | `0` / `0` |
| `consumerProgress`<br>无 `-g` 在线汇总样本 | 3929 | 4210 | 0.93x / -7.2% | 12.8 | 307x / 99.7% | `0` / `0` |
| `consumerProgress`<br>`-c DefaultCluster` | 4355 | 1464 | 2.97x / 66.4% | 10.4 | 419x / 99.8% | `0` / `0` |
| `consumerProgress`<br>`-g <consumer_group> -s true` | 4048 | 34.2 | 118x / 99.2% | 12.8 | 316x / 99.7% | `0` / `0` |
| `consumerConnection`<br>`-g <consumer_group>` | 3724 | 1358 | 2.74x / 63.5% | 6.6 | 564x / 99.8% | `0` / `0` |
| `consumerStatus`<br>`-g <sample>` 列表/文件模式 | 4366 | 1487 | 2.94x / 65.9% | 15.6 | 280x / 99.6% | `0*` / `0*` |
| `consumerStatus -i`<br>`-g <sample> -i <clientId>` | 4655 | 56 | 83.1x / 98.8% | 18.8 | 248x / 99.6% | `0*` / `0*` |
| `consumerStatus -i -b`<br>`-g <sample> -i <clientId> -b <broker>` | 3873 | 71.6 | 54.1x / 98.2% | 26 | 149x / 99.3% | `0*` / `0*` |
| `statsAll`<br>`-t <sample>` | 4594 | 146 | 31.5x / 96.8% | 129 | 35.6x / 97.2% | `0` / `0` |
| `allocateMQ`<br>`-t <sample> -i <ip>,<ip>` | 4206 | 4281 | 0.98x / -1.8% | 139 | 30.3x / 96.7% | `0` / `0` |
| `printMsgByQueue`<br>`-t <sample> -a <broker> -i 0 -b <timestamp> -e <timestamp> -p true -d false` | 4086 | 4565 | 0.9x / -11.7% | 133 | 30.7x / 96.7% | `0` / `0` |
| `printMsgByQueue -f`<br>`-t <sample> -a <broker> -i 0 -p false -f true` | 3985 | 46.2 | 86.3x / 98.8% | 10.8 | 369x / 99.7% | `0` / `0` |
| `printMsg`<br>`-t <sample> -b <timestamp> -e <timestamp> -d false` | 4074 | 49.4 | 82.5x / 98.8% | 19.2 | 212x / 99.5% | `0` / `0` |
| `producer`<br>`-b <broker>` | 4370 | 21.2 | 206x / 99.5% | 5.4 | 809x / 99.9% | `0*` / `0` |
| `consumeMessage`<br>`-t <sample> -b <broker> -i 0 -o 0 -c 1` | 4436 | 173 | 25.6x / 96.1% | 9.2 | 482x / 99.8% | `0` / `0` |
| `getColdDataFlowCtrInfo`<br>`-b <broker>`；`-c DefaultCluster` 也已对照 | 4955 | 1423 | 3.48x / 71.3% | 5 | 991x / 99.9% | `0` / `0` |
| `exportConfigs`<br>`-c DefaultCluster -f /tmp/<sample>` | 4213 | 27.6 | 153x / 99.3% | 9.6 | 439x / 99.8% | `0` / `0` |
| `exportMetadata`<br>`-c DefaultCluster -f /tmp/<sample>`；`-b -t/-g` 也已对照 | 3891 | 2776 | 1.4x / 28.7% | 8.4 | 463x / 99.8% | `0` / `0` |
| `exportMetrics`<br>`-c DefaultCluster -f /tmp/<sample>` | 4294 | 43.8 | 98x / 99% | 21.8 | 197x / 99.5% | `0` / `0` |
| `checkRocksdbCqWriteProgress`<br>`-c DefaultCluster -t <sample> -cf 0` | 4144 | 1510 | 2.74x / 63.6% | 6 | 691x / 99.9% | `0` / `0` |
| `rocksDBConfigToJson`<br>`-c DefaultCluster -t topics`；`-b` 与默认 `-t` 也已对照 | 3963 | 34.4 | 115x / 99.1% | 11.2 | 354x / 99.7% | `0` / `0` |
| `exportPopRecord`<br>`-c DefaultCluster -d false`；`-b` dry-run 与默认 actual 也已对照 | 3858 | 22 | 175x / 99.4% | 5 | 772x / 99.9% | `0` / `0` |
| `updateKvConfig`<br>`-s <sample> -k <temp> -v value` | 4858 | 19.2 | 253x / 99.6% | 6 | 810x / 99.9% | `0` / `0` |
| `deleteKvConfig`<br>`-s <sample> -k <temp>` | 4759 | 1821 | 2.61x / 61.7% | 6.8 | 700x / 99.9% | `0` / `0` |
| `updateTopic`<br>cluster/broker 临时 Topic | 4257 | 30.2 | 141x / 99.3% | 9.8 | 434x / 99.8% | `0` / `0` |
| `deleteTopic`<br>cluster 临时 Topic 清理 | 4079 | 26.8 | 152x / 99.3% | 12 | 340x / 99.7% | `0` / `0` |
| `updateSubGroup`<br>`-c DefaultCluster` 临时订阅组；`-b` 也已对照 | 4351 | 19.4 | 224x / 99.6% | 7.8 | 558x / 99.8% | `0` / `0` |
| `deleteSubGroup`<br>`-c DefaultCluster` 临时订阅组；`-b` 也已对照 | 4070 | 34.4 | 118x / 99.2% | 11.2 | 363x / 99.7% | `0` / `0` |
| `updateOrderConf put`<br>`-m put -t <sample> -v <broker>:1` | 4393 | 1595 | 2.75x / 63.7% | 5 | 879x / 99.9% | `0` / `0` |
| `updateOrderConf get`<br>`-m get -t <sample>` | 4257 | 11.8 | 361x / 99.7% | 6 | 710x / 99.9% | `0` / `0` |
| `updateOrderConf delete`<br>`-m delete -t <sample>` | 3777 | 12 | 315x / 99.7% | 4.6 | 821x / 99.9% | `0` / `0` |
| `updateBrokerConfig`<br>`-b <broker> -k enableDetailStat -v true`；`-c DefaultCluster` 也已对照 | 4590 | 1861 | 2.47x / 59.5% | 5.6 | 820x / 99.9% | `0` / `0` |
| `updateNamesrvConfig`<br>`-n <broker> -k clusterTest -v false` | 4856 | 18.2 | 267x / 99.6% | 11 | 441x / 99.8% | `0` / `0` |
| `updateTopicPerm`<br>`-c DefaultCluster -t <temp> -p 4`；`-b <route-master>`, same-perm 与非 master 错误也已对照 | 3742 | 20.6 | 182x / 99.4% | 7 | 535x / 99.8% | `0` / `0` |
| `setConsumeMode`<br>`-c DefaultCluster -t <sample> -g <sample> -m POP -q 1`；`-b <broker>` 也已对照 | 4693 | 22 | 213x / 99.5% | 8.6 | 546x / 99.8% | `0` / `0` |
| `updateColdDataFlowCtrGroupConfig`<br>`-c DefaultCluster -g <sample> -v <threshold>`；`-b <broker>` 也已对照 | 4803 | 16 | 300x / 99.7% | 7.6 | 632x / 99.8% | `0` / `0` |
| `removeColdDataFlowCtrGroupConfig`<br>`-c DefaultCluster -g <sample>`；`-b <broker>` 也已对照 | 3708 | 1353 | 2.74x / 63.5% | 8.2 | 452x / 99.8% | `0` / `0` |
| `updateTopicList`<br>`-c DefaultCluster -f <TopicConfig JSON>`；`-b <broker>` 也已对照 | 4048 | 2674 | 1.51x / 33.9% | 16.6 | 244x / 99.6% | `0` / `0` |
| `updateSubGroupList`<br>`-c DefaultCluster -f <SubscriptionGroupConfig JSON>`；`-b <broker>` 也已对照 | 3772 | 19.8 | 191x / 99.5% | 8.8 | 429x / 99.8% | `0` / `0` |
| `cleanExpiredCQ`<br>`-c DefaultCluster`；`-b <broker>` 也已对照 | 4604 | 1789 | 2.57x / 61.1% | 11.6 | 397x / 99.7% | `0` / `0` |
| `cleanUnusedTopic`<br>`-c DefaultCluster`；`-b <broker>` 也已对照 | 4738 | 34 | 139x / 99.3% | 9 | 526x / 99.8% | `0` / `0` |
| `deleteExpiredCommitLog`<br>`-c DefaultCluster`；`-b <broker>` 也已对照 | 4021 | 1385 | 2.9x / 65.6% | 8 | 503x / 99.8% | `0` / `0` |
| `wipeWritePerm`<br>`-n <broker> -b <broker>` | 4923 | 23.4 | 210x / 99.5% | 7.4 | 665x / 99.8% | `0` / `0` |
| `addWritePerm`<br>`-n <broker> -b <broker>` | 3840 | 22.8 | 168x / 99.4% | 7.6 | 505x / 99.8% | `0` / `0` |
| `cloneGroupOffset`<br>`-s <consumer_group> -d <tempGroup> -t <sample> -o true` | 3775 | 1450 | 2.6x / 61.6% | 11.6 | 325x / 99.7% | `0` / `0` |
| `cloneGroupOffset -o true`<br>`-s <consumer_group> -d <sample> -t %RETRY%<consumer_group> -o true` | 3854 | 35.6 | 108x / 99.1% | 9.2 | 419x / 99.8% | `0` / `0` |
| `cloneGroupOffset -o`<br>missing offline value parser preflight | 431 | 4.6 | 93.7x / 98.9% | 3.4 | 127x / 99.2% | `0` / `0` |
| `sendMessage`<br>`-t <sample> -b <broker> -i 0` | 3723 | 34.4 | 108x / 99.1% | 7.8 | 477x / 99.8% | `0*` / `0*` |
| `sendMessage -m true`<br>`-t <sample> -b <broker> -i 0 -m true` | 4173 | 158 | 26.4x / 96.2% | 134 | 31.1x / 96.8% | `0*` / `0*` |
| `sendMsgStatus`<br>`-b <broker> -s 16 -c 1` | 4501 | 1392 | 3.23x / 69.1% | 8.8 | 511x / 99.8% | `0*` / `0*` |
| `checkMsgSendRT`<br>`-t <sample> -s 16 -a 2` | 4580 | 35 | 131x / 99.2% | 11.8 | 388x / 99.7% | `0*` / `0*` |
| `resetMasterFlushOffset`<br>`-b <broker> -o 0` | 4739 | 3331 | 1.42x / 29.7% | 6.6 | 718x / 99.9% | `0` / `0` |
| `resetOffsetByTime`<br>`-b <broker> -q 0 -o 1 -s <timestamp>` | 4247 | 16 | 265x / 99.6% | 5.6 | 758x / 99.9% | `0` / `0` |
| `resetOffsetByTime`<br>`-g <tempGroup> -t <sample> -s -1 -f true` | 4491 | 1369 | 3.28x / 69.5% | 7.6 | 591x / 99.8% | `0` / `0` |
| `resetOffsetByTime`<br>`-b <broker> -q 0 -s 9999999999999` | 3746 | 19.2 | 195x / 99.5% | 8 | 468x / 99.8% | `0` / `0` |
| `resetOffsetByTime`<br>`-b <broker> -s -1` | 4463 | 4548 | 0.98x / -1.9% | 14 | 319x / 99.7% | `0` / `0` |
| `skipAccumulatedMessage`<br>`-g <tempGroup> -t <sample>` | 5232 | 27 | 194x / 99.5% | 7.4 | 707x / 99.9% | `0` / `0` |
| `updateStaticTopic`<br>`-b <broker> -qn 4 -t <tempTopic>` | 4355 | 44 | 99x / 99% | 10.4 | 419x / 99.8% | `0` / `0` |
| `updateStaticTopic -mf`<br>`-b <broker> -qn 4 -mf /tmp/<sample>` | 4805 | 19.6 | 245x / 99.6% | 9.2 | 522x / 99.8% | `0*` / `0*` |
| `remappingStaticTopic`<br>`-b <broker> -t <tempStaticTopic>` | 4284 | 3242 | 1.32x / 24.3% | 8.8 | 487x / 99.8% | `0` / `0` |
| `remappingStaticTopic -mf`<br>`-b <broker> -mf /tmp/<sample>` | 4507 | 1767 | 2.55x / 60.8% | 10.8 | 417x / 99.8% | `0*` / `0*` |
| `clusterRT`<br>`-a 2 -s 16 -i 1` | 2066 | 1033 | 2x / 50% | 1074 | 1.92x / 48% | `0` / `0` |
| `getControllerMetaData`<br>`-a 127.0.0.1:<port>` | 7185 | 15 | 479x / 99.8% | 6 | 1198x / 99.9% | `0` / `0` |
| `getControllerConfig`<br>`-a 127.0.0.1:<port>` | 7260 | 40.8 | 178x / 99.4% | 12.8 | 567x / 99.8% | `0` / `0` |
| `getSyncStateSet`<br>`-a 127.0.0.1:<port> -b broker-a` | 7265 | 18.2 | 399x / 99.7% | 7.4 | 982x / 99.9% | `0` / `0` |
| `dumpCompactionLog`<br>`-f /tmp/<sample>` | 440 | 11.2 | 39.3x / 97.5% | 4 | 110x / 99.1% | `0` / `0` |
| `exportMetadataInRocksDB`<br>`-p /tmp/<sample> -t topics` | 417 | 6.2 | 67.3x / 98.5% | 3.4 | 123x / 99.2% | `0` / `0` |
| `exportMetadataInRocksDB`<br>`-p /tmp/<sample> -t topics` | 981 | 32.8 | 29.9x / 96.7% | 11 | 89.2x / 98.9% | `0` / `0` |
| `exportMetadataInRocksDB -j true`<br>`-p /tmp/<sample> -t subscriptionGroups -j true` | 1028 | 23.8 | 43.2x / 97.7% | 9.4 | 109x / 99.1% | `0` / `0` |
| `rocksDBConfigToJson`<br>`-p /tmp/<sample> -t topics` | 666 | 13.6 | 49x / 98% | 3.2 | 208x / 99.5% | `0` / `0` |
| `rocksDBConfigToJson -j false`<br>`-p /tmp/<sample> -t subscriptionGroups -j false` | 665 | 12.4 | 53.6x / 98.1% | 3.2 | 208x / 99.5% | `0` / `0` |
| `rocksDBConfigToJson -e`<br>`-p /tmp/<sample> -t topics -j false -e <file>` | 674 | 14.4 | 46.8x / 97.9% | 3.6 | 187x / 99.5% | `0` / `0` |
| `rocksDBConfigToJson`<br>`-p /tmp/<sample> -t consumerOffsets` | 927 | 18.2 | 50.9x / 98% | 5.4 | 172x / 99.4% | `0` / `0` |
| `rocksDBConfigToJson -j false`<br>`-p /tmp/<sample> -t consumerOffsets -j false` | 1021 | 20.2 | 50.5x / 98% | 5.8 | 176x / 99.4% | `0` / `0` |
| `rocksDBConfigToJson -e`<br>`-p /tmp/<sample> -t consumerOffsets -j false -e <file>` | 1044 | 32.6 | 32x / 96.9% | 5 | 209x / 99.5% | `0` / `0` |
| `updateControllerConfig`<br>`-a 127.0.0.1:<port> -k controllerDLegerGroup -v group1` | 7434 | 28 | 266x / 99.6% | 6.8 | 1093x / 99.9% | `0` / `0` |
| `removeBroker`<br>`-c 127.0.0.1:<port> -b DefaultCluster:broker-a:-1` | 9140 | 9393 | 0.97x / -2.8% | 3 | 3047x / 100% | `0` / `0` |
| `removeBroker`<br>`-c 127.0.0.1:<port> -b DefaultCluster:<sample>:0 --timeout-ms 60000` | 13264 | 8536 | 1.55x / 35.6% | 8529 | 1.56x / 35.7% | `0` / `0` |
| `addBroker`<br>`-c 127.0.0.1:<port> -b /tmp/<sample>*.conf` | 7948 | 123 | 64.6x / 98.5% | 103 | 77.2x / 98.7% | `0` / `0` |
| `getBrokerEpoch`<br>`-n <broker> -b <sample>` | 4255 | 20.8 | 205x / 99.5% | 6.2 | 686x / 99.9% | `0` / `0` |
| `getBrokerEpoch -c`<br>`-n <broker> -c <sample>` | 4133 | 20 | 207x / 99.5% | 6.4 | 646x / 99.8% | `0` / `0` |
| `cleanBrokerMetadata`<br>`-a 127.0.0.1:<port> -c <sample> -bn <sample> -b 0` | 6522 | 23.8 | 274x / 99.6% | 11.4 | 572x / 99.8% | `0` / `0` |
| `electMaster`<br>`-a 127.0.0.1:<port> -c <sample> -bn <sample> -b 3` | 8484 | 19.6 | 433x / 99.8% | 10.2 | 832x / 99.9% | `0*` / `0*` |
| `getSyncStateSet -c`<br>`-n <broker> -a 127.0.0.1:<port> -c <sample>` | 4519 | 1447 | 3.12x / 68% | 14.6 | 310x / 99.7% | `0` / `0` |
| `listUser`<br>`-b 127.0.0.1:<port> -f <sample>` | 8024 | 3119 | 2.57x / 61.1% | 6.4 | 1254x / 99.9% | `0` / `0` |
| `listUser -c`<br>`-n <broker> -c <sample> -f <sample>` | 4418 | 19.8 | 223x / 99.6% | 7 | 631x / 99.8% | `0` / `0` |
| `getUser`<br>`-b 127.0.0.1:<port> -u <sample>` | 7429 | 29 | 256x / 99.6% | 6.2 | 1198x / 99.9% | `0` / `0` |
| `getUser -c`<br>`-n <broker> -c <sample> -u <sample>` | 4441 | 23.8 | 187x / 99.5% | 7.4 | 600x / 99.8% | `0` / `0` |
| `createUser`<br>`-b 127.0.0.1:<port> -u <sample> -p <secret> -t Super` | 6847 | 23 | 298x / 99.7% | 7 | 978x / 99.9% | `0` / `0` |
| `createUser -c`<br>`-n <broker> -c <sample> -u <sample> -p <secret> -t Super` | 3809 | 25.6 | 149x / 99.3% | 8.6 | 443x / 99.8% | `0` / `0` |
| `updateUser`<br>`-b 127.0.0.1:<port> -u <sample> -s disable` | 6965 | 1776 | 3.92x / 74.5% | 7.6 | 916x / 99.9% | `0` / `0` |
| `updateUser -c`<br>`-n <broker> -c <sample> -u <sample> -s disable` | 3895 | 23.8 | 164x / 99.4% | 9.4 | 414x / 99.8% | `0` / `0` |
| `deleteUser`<br>`-b 127.0.0.1:<port> -u <sample>` | 6833 | 1447 | 4.72x / 78.8% | 11.2 | 610x / 99.8% | `0` / `0` |
| `deleteUser -c`<br>`-n <broker> -c <sample> -u <sample>` | 3789 | 23 | 165x / 99.4% | 12.8 | 296x / 99.7% | `0` / `0` |
| `setCommitLogReadAheadMode`<br>`-b 127.0.0.1:<port> -m 0` | 6691 | 14 | 478x / 99.8% | 5.8 | 1154x / 99.9% | `0` / `0` |
| `setCommitLogReadAheadMode -c`<br>`-n <broker> -c <sample> -m 1` | 3753 | 1323 | 2.84x / 64.7% | 8 | 469x / 99.8% | `0` / `0` |
| `copyUser`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port> -u <sample>,<sample>` | 7026 | 1478 | 4.75x / 79% | 34.2 | 205x / 99.5% | `0` / `0` |
| `copyUser`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port>` | 7972 | 1491 | 5.35x / 81.3% | 39.6 | 201x / 99.5% | `0` / `0` |
| `createAcl`<br>`-b 127.0.0.1:<port> -s User:<sample> -r Topic:<sample> -a Pub,Sub -i <ip>,<ip> -d Allow` | 4275 | 18.6 | 230x / 99.6% | 8.8 | 486x / 99.8% | `0` / `0` |
| `createAcl -c`<br>`-n <broker> -c <sample> -s User:<sample> -r Topic:<sample> -a Pub,Sub -i <ip>,<ip> -d Allow` | 3926 | 26.6 | 148x / 99.3% | 10.4 | 378x / 99.7% | `0` / `0` |
| `updateAcl`<br>`-b 127.0.0.1:<port> -s User:<sample> -r Topic:<sample> -a Sub -d Deny` | 3894 | 17.2 | 226x / 99.6% | 6.4 | 608x / 99.8% | `0` / `0` |
| `updateAcl -c`<br>`-n <broker> -c <sample> -s User:<sample> -r Topic:<sample> -a Sub -d Deny` | 4506 | 35.8 | 126x / 99.2% | 16.2 | 278x / 99.6% | `0` / `0` |
| `deleteAcl`<br>`-b 127.0.0.1:<port> -s User:<sample> -r Topic:<sample>` | 4561 | 29.8 | 153x / 99.3% | 6.6 | 691x / 99.9% | `0` / `0` |
| `deleteAcl -c`<br>`-n <broker> -c <sample> -s User:<sample> -r Topic:<sample>` | 3977 | 19.4 | 205x / 99.5% | 8.4 | 473x / 99.8% | `0` / `0` |
| `getAcl`<br>`-b 127.0.0.1:<port> -s User:<sample>` | 4679 | 2724 | 1.72x / 41.8% | 5.8 | 807x / 99.9% | `0` / `0` |
| `getAcl -c`<br>`-n <broker> -c <sample> -s User:<sample>` | 3755 | 1370 | 2.74x / 63.5% | 7.6 | 494x / 99.8% | `0` / `0` |
| `listAcl`<br>`-b 127.0.0.1:<port> -s User:<sample>` | 3769 | 19.2 | 196x / 99.5% | 5.8 | 650x / 99.8% | `0` / `0` |
| `listAcl -c`<br>`-n <broker> -c <sample> -s User:<sample>` | 3789 | 18.8 | 202x / 99.5% | 9.6 | 395x / 99.7% | `0` / `0` |
| `copyAcl`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port> -s User:<sample>,User:<sample>` | 4188 | 1411 | 2.97x / 66.3% | 13 | 322x / 99.7% | `0` / `0` |
| `copyAcl update`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port> -s User:<sample>,User:<sample>` | 4196 | 35.8 | 117x / 99.1% | 12.8 | 328x / 99.7% | `0` / `0` |
| `copyAcl`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port>` | 3874 | 40.8 | 95x / 98.9% | 14.2 | 273x / 99.6% | `0` / `0` |
| `copyAcl update`<br>`-f 127.0.0.1:<port> -t 127.0.0.1:<port>` | 3970 | 2840 | 1.4x / 28.5% | 15.6 | 254x / 99.6% | `0` / `0` |
| `updateAclConfig`<br>`-b 127.0.0.1:<port> -a <sample> -s <secret> -w <ip-pattern> -t PUB\\|SUB -g SUB -m true` | 10115 | 27.6 | 366x / 99.7% | 9.2 | 1099x / 99.9% | `0` / `0` |
| `updateAclConfig -c`<br>`-n <broker> -c <sample> -a <sample> -s <secret> -w <ip-pattern> -t PUB\\|SUB -g SUB -m true` | 4718 | 29.4 | 160x / 99.4% | 12.6 | 374x / 99.7% | `0` / `0` |
| `deleteAclConfig`<br>`-b 127.0.0.1:<port> -a <sample>` | 12095 | 1404 | 8.61x / 88.4% | 8.4 | 1440x / 99.9% | `0` / `0` |
| `deleteAclConfig -c`<br>`-n <broker> -c <sample> -a <sample>` | 4391 | 1406 | 3.12x / 68% | 9.6 | 457x / 99.8% | `0` / `0` |
| `deleteAccessConfig`<br>`-b 127.0.0.1:<port> -a <sample>` | 10343 | 1391 | 7.44x / 86.6% | 7.8 | 1326x / 99.9% | `0` / `0` |
| `updateGlobalWhiteAddr`<br>`-b 127.0.0.1:<port> -g <ip-pattern> -p /tmp/<sample>` | 11006 | 18.4 | 598x / 99.8% | 7.4 | 1487x / 99.9% | `0` / `0` |
| `updateGlobalWhiteAddr -c`<br>`-n <broker> -c <sample> -g <ip-pattern> -p /tmp/<sample>` | 4398 | 29.2 | 151x / 99.3% | 9.4 | 468x / 99.8% | `0` / `0` |
| `startMonitoring`<br>`-n <broker>` under external `timeout 2` | 2508 | 2145 | 1.17x / 14.5% | 2159 | 1.16x / 13.9% | `0` / `0` |
| `getBrokerEpoch -i`<br>`-n <broker> -c <sample> -i 1` | 2116 | 1270 | 1.67x / 40% | 1068 | 1.98x / 49.5% | `0` / `0` |
| `getSyncStateSet -i`<br>`-n <broker> -a 127.0.0.1:<port> -c <sample> -i 1` | 1774 | 15907 | 0.11x / -796.7% | 1066 | 1.66x / 39.9% | `0` / `0` |
| `haStatus -i`<br>`-b <broker> -i 1` | 5035 | 2824 | 1.78x / 43.9% | 1016 | 4.96x / 79.8% | `0` / `0` |
| `clusterAclConfigVersion`<br>`-n <broker> -c DefaultCluster`；`-b 127.0.0.1:<port>` ACL broker 也已实测 | 3722 | 24 | 155x / 99.4% | 2.8 | 1329x / 99.9% | `0` / `0` |
