package com.bigdata.train.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*读取mysql*/
public class RunningDataMysqlCDC {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.0.162", 8081, "E:\\GitHub\\flink-train\\target\\flink-train-1.0-jar-with-dependencies.jar");
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().inStreamingMode().build();
        env.setParallelism(1);
        //2.Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点续传,需要从 Checkpoint 或者 Savepoint 启动程序
        //2.1 开启 Checkpoint,每隔 5 秒钟做一次 CK
        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
         //2.4 指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://nameservice1:8020/flinkCDC"));
        //2.6 设置访问 HDFS 的用户名
        System.setProperty("HADOOP_USER_NAME", "flink");
        TableEnvironment tEnv = TableEnvironment.create(settings);

        String ddlSource = "CREATE TABLE mysq_test_source_cdc_3 (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts varchar\n" +
                ") \n" +
                "WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '192.168.0.8',\n" +
                "  'port' = '3306',\n" +
                "  'database-name' = 'dolphinscheduler',\n" +
                "  'table-name' = 'user_log', \n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root' \n" +
                ")";

        String ddlSink = "CREATE TABLE user_log_cdc_3 (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts varchar\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka', \n" +
                "    'topic' = 'lzy-topic1', \n" +
                "    'properties.bootstrap.servers' = '192.168.0.163:9092,192.168.0.164:9092,192.168.0.165:9092', \n" +
                "    'properties.group.id' = 'LocalRunningDataMysql', \n" +
                "    'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                "    'properties.sasl.mechanism' = 'GSSAPI',\n" +
                "    'properties.sasl.kerberos.service.name' = 'kafka',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'format' = 'canal-json', \n" +
                "    'json.ignore-parse-errors' = 'true' \n" +
                ")";
        //提取mysq的数据，写入到 kafka
        String sql = "insert into user_log_cdc_3  select * from mysq_test_source_cdc_3";

        tEnv.executeSql(ddlSource);
        tEnv.executeSql(ddlSink);
        tEnv.executeSql(sql);
        env.execute("LocalRunningDataMysql");
    }
}
