package com.bigdata.train.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RunningMysqlCDCToKafka {

    public static void main(String[] args) throws Exception {

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.0.8")
                .port(3306)
                .databaseList("dolphinscheduler") // set captured database
                .tableList("dolphinscheduler.user_log") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        //创建Flink-MySQL-CDC的Source
        /*MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder().hostname("192.168.0.8")
                        .port(3306).username("root").password("root")
                        .databaseList("dolphinscheduler")
                        .tableList("dolphinscheduler.user_log")
                        .startupOptions(StartupOptions.latest())
//                .startupOptions(KafkaOptions.StartupOptions.class)
                        .deserializer(new DebeziumDeserializationSchema<String>() {
                            //自定义数据解析器
                            @Override
                            public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

                                //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink.z_user_info
                                String topic = sourceRecord.topic();
                                String[] arr = topic.split("\\.");
                                String db = arr[1];
                                String tableName = arr[2];

                                //获取操作类型 READ DELETE UPDATE CREATE
                                Envelope.Operation operation = Envelope.operationFor(sourceRecord);

                                //获取值信息并转换为Struct类型
                                Struct value = (Struct) sourceRecord.value();

                                //获取变化后的数据
                                Struct after = value.getStruct("after");

                                //创建JSON对象用于存储数据信息
                                JSONObject data = new JSONObject();
                                if (after != null) {
                                    Schema schema = after.schema();
                                    for (Field field : schema.fields()) {
                                        data.put(field.name(), after.get(field.name()));
                                    }
                                }

                                //创建JSON对象用于封装最终返回值数据信息
                                JSONObject result = new JSONObject();
                                result.put("operation", operation.toString().toLowerCase());
                                result.put("data", data);
                                result.put("database", db);
                                result.put("table", tableName);
                                //发送数据至下游
                                collector.collect(result.toString());
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }).build();*/


        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("192.168.0.165", 8081, "E:\\GitHub\\flink-train\\target\\flink-train-1.0-jar-with-dependencies.jar");
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(3000);

        //使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                /*.setParallelism(4)*/;
        //打印数据
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink("lzy-topic1"));
        //执行任务
        env.execute("RunningMysqlCDCToKafka");
    }
}
