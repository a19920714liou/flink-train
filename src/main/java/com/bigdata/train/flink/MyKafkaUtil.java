package com.bigdata.train.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

public class MyKafkaUtil {
    public static FlinkKafkaProducer getKafkaSink(String topicName) {
        //增加Kerberos认证
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.0.163:9092,192.168.0.164:9092,192.168.0.165:9092");
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "GSSAPI");
        properties.setProperty("sasl.kerberos.service.name", "kafka");
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                topicName,
                new SimpleStringSchema(),
                properties);

        return myProducer;
    }
    /*public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic(topic)
                .build();
        //增加Kerberos认证
        Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "GSSAPI");
        properties.setProperty("sasl.kerberos.service.name", "kafka");
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(properties)
                .setBootstrapServers("192.168.0.163:9092,192.168.0.164:9092,192.168.0.165:9092")
                .setRecordSerializer(serializer)
                .build();
        return sink;
    }*/
}
