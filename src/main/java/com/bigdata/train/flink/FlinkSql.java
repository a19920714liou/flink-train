package com.bigdata.train.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkSql {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env);

        String ddlSource = "CREATE TABLE user_log (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts varchar\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka', \n" +
                "    'connector.version' = 'universal',  \n" +
                "    'connector.topic' = 'user_behavior', \n" +
                "    'connector.startup-mode' = 'earliest-offset', \n" +
                "    'connector.properties.0.key' = 'zookeeper.connect', \n" +
                "    'connector.properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                "     'connector.properties.sasl.mechanism' = 'GSSAPI',\n" +
                "     'connector.properties.sasl.kerberos.service.name' = 'kafka',\n" +
                "    'connector.properties.0.value' = '192.168.0.163:2181,192.168.0.164:2181,192.168.0.165:2181', \n" +
                "    'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "    'connector.properties.1.value' = '192.168.0.163:9092,192.168.0.164:9092,192.168.0.165:9092', \n" +
                "    'update-mode' = 'append',\n" +
                "    'format.type' = 'json', \n" +
                "    'format.derive-schema' = 'true' \n" +
                ")";

        String ddlSink = "CREATE TABLE doris_test_sink_1 (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts varchar\n" +
                ") \n" +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '192.168.0.160:18030',\n" +
                "  'table.identifier' = 'test.user_log',\n" +
                "  'sink.batch.size' = '2',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'By7KJ6PjufeHMS2a'\n" +
                ")";

        //提取读取到的数据，写入到 DORIS
        String sql = "insert into doris_test_sink_1 select * from user_log";

        System.out.println(ddlSource);
        System.out.println(ddlSink);
        blinkStreamTableEnv.executeSql(ddlSource);
        blinkStreamTableEnv.executeSql(ddlSink);
        TableResult tableResult1 = blinkStreamTableEnv.executeSql(sql);
        // 通过 TableResult 来获取作业状态
        System.out.println(tableResult1.getJobClient().get().getJobStatus());
        try {
            env.execute("Blink Stream SQL Job —— read data from kafka，sink to doris");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
