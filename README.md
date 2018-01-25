# alicloud-hbase-spark-examples
1、在examples/conf/hbase-site.xml中配置alihbase的zkurl

2、下载alihbase以及aliphoenix的客户端，里面包含spark分析hbase数据时需要的jar包

3、eg：

a、spark-submit  --class org.apache.spark.hbase.NativeRDDAnalyze --master yarn-client --jars shc-core-1.1.0-2.1-s_2.11.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-common-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-server-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-client-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-protocol-1.1.1.jar  shc-examples-1.1.0-2.1-s_2.11.jar 

b、spark-submit  --class org.apache.spark.sql.execution.datasources.hbase.SqlAnalyze --master yarn-client --jars shc-core-1.1.0-2.1-s_2.11.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-common-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-server-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-client-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-protocol-1.1.1.jar   shc-examples-1.1.0-2.1-s_2.11.jar 

c、spark-submit  --class org.apache.spark.phoenix.connector.PhoenixTest --master yarn-client --jars shc-core-1.1.0-2.1-s_2.11.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-common-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-server-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-client-1.1.1.jar,/home/hadoop/muyuan/alihbase-1.1.1/lib/alihbase-protocol-1.1.1.jar,/home/hadoop/muyuan/phoenix-4.11.0-AliHBase-1.1-0.3/phoenix-4.11.0-AliHBase-1.1-0.3-server.jar,/home/hadoop/muyuan/phoenix-4.11.0-AliHBase-1.1-0.3/phoenix-spark-4.11.0-HBase-1.1.jar   shc-examples-1.1.0-2.1-s_2.11.jar 
