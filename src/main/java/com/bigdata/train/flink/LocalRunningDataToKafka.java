package com.bigdata.train.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static com.bigdata.train.flink.WordCount.outputTopic;

public class LocalRunningDataToKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); //采用默认配置


        // 设定数据来源为集合数据
        DataStream<LocalRunningDataSourceTest.Person> flintstones = env.
                fromElements(new LocalRunningDataSourceTest.Person("Fred", 35),
                        new LocalRunningDataSourceTest.Person("Wilma", 35),
                        new LocalRunningDataSourceTest.Person("Pebbles", 2));

        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic(outputTopic)
                .build();
        //生产者
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.0.201:9092,192.168.0.203:9092,192.168.0.204:9092")
                .setRecordSerializer(serializer)
                .build();

        DataStream<String> adults = flintstones
                .filter((FilterFunction<LocalRunningDataSourceTest.Person>) person -> person.getAge() >= 18)
                .flatMap((FlatMapFunction<LocalRunningDataSourceTest.Person, String>) (person, collector) -> {
                    System.out.println(person.getName());
                    collector.collect(person.getName());
                }).returns(String.class);
        adults.sinkTo(sink);
        env.execute("LocalRunningDataToKafka");
    }
}
