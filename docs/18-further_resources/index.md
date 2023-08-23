# 更多资源

我希望本书对你有所帮助，并且你现在对查询引擎内部结构应该有了更好的理解。如果你觉得有一些主题没有被充分纳入，或者根本没有，我很乐意听你告知，这样我就可以考虑在这本书未来的修订版中添加其它内容。

反馈可以发布在 [LeanPub 网站](https://community.leanpub.com/t/feedback/2160) 的公共论坛上，也可以通过 [@andygrove_io](https://twitter.com/andygrove_io) 直接向我发送消息。

## Open-Source Projects 开源项目

有许多包含查询引擎的开源项目，并且使用这些项目有益于了解相关主题。下面是一些流行的开源查询引擎举例：

- [Apache Arrow](https://arrow.apache.org/)
- [Apache Calcite](https://calcite.apache.org/)
- [Apache Drill](https://drill.apache.org/)
- [Apache Hadoop](https://hadoop.apache.org/)
- [Apache Hive](https://hive.apache.org/)
- [Apache Impala](https://impala.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [Facebook Presto](https://prestodb.io/)
- [NVIDIA RAPIDS Accelerator for Apache Spark](https://nvidia.github.io/spark-rapids/)

## YouTube

我最近才发现 Andy Pavlo 的系列讲座可以在 YouTube 上找到 ([这里](https://www.youtube.com/playlist?list=PLSE8ODhjZXjasmrEd2_Yi1deeE360zv5O))。其中涵盖的不仅仅是查询引擎，还有关于查询优化和执行的更广泛的内容。我强烈推荐大家观看这些视频。

## Sample Data 样本数据

较早的章节参考了[纽约市出租车和豪华轿车委员会旅行记录数据集](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)。黄色和绿色的出租车旅行记录包括捕获的上下车日期/时间、接送地点、旅行距离、列出票价、费率类型、付费类型以及驾驶员报告的乘客数量等字段。数据以 CSV 格式提供。KQuery 项目包含用于将这些 CSV 文件转换为 Parquet 格式的源代码。

数据可以通过网站连接或者直接访问 S3 下载文件。例如，Linux 或者 Mac 用户可以使用 `curl` 或 `wget` 通过以下命令下载黄色出租车 2019 年 1 月的数据，并根据文件名约定创建脚本下载其它文件。

```shell
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv
```
