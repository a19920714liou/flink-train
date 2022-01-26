package com.bigdata.train.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*读取mysql*/
public class LocalRunningDataMysql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        String ddlSource = "CREATE TABLE user_log (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts varchar\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka', \n" +
                "    'topic' = 'test', \n" +
                "    'properties.bootstrap.servers' = '192.168.0.201:9092,192.168.0.203:9092,192.168.0.204:9092', \n" +
                "    'properties.group.id' = 'LocalRunningDataMysql', \n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json', \n" +
                "    'json.ignore-parse-errors' = 'true' \n" +
                ")";

        String ddlSink = "CREATE TABLE mysq_test_sink_1 (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts varchar\n" +
                ") \n" +
                "WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://192.168.0.108:23306/dolphinscheduler2?useUnicode=true&characterEncoding=utf-8', \n" +
                "  'driver' = 'com.mysql.jdbc.Driver', \n" +
                "  'table-name' = 'user_log', \n" +
                "  'username' = 'root',\n" +
                "  'password' = 'root', \n" +
                "  'scan.fetch-size' = '3' \n" +
                ")";

        String ddlKafkaSink = "CREATE TABLE user_log2 (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts varchar\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka', \n" +
                "    'topic' = 'test2', \n" +
                "    'properties.bootstrap.servers' = '192.168.0.201:9092,192.168.0.203:9092,192.168.0.204:9092', \n" +
                "    'properties.group.id' = 'LocalRunningDataMysql', \n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json', \n" +
                "    'json.ignore-parse-errors' = 'true' \n" +
                ")";
        //提取读取到的数据，写入到 DORIS
        String sql = "insert into mysq_test_sink_1 select * from user_log";

        tEnv.executeSql(ddlSource);
        tEnv.executeSql(ddlSink);
        tEnv.executeSql(ddlKafkaSink);
        //tEnv.executeSql("select * from user_log").print();
        tEnv.executeSql(sql);
        tEnv.executeSql("insert into user_log2 select * from mysq_test_sink_1");
       // env.execute("LocalRunningDataMysql");
    }
}
