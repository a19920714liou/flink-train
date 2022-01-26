package com.bigdata.train.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class WordCount {
    // kafka-topics --zookeeper work01:2181 work02:2181 work03:2181 --create --replication-factor 1 --partitions 1 --topic lzy-topic1
    //kafka-console-producer --broker-list  192.168.1.211:9092 --topic lzy-topic1
    final static String inputTopic = "lzy-topic1";
    //kafka-console-consumer  --bootstrap-server  192.168.1.211:9092 --topic lzy-topic2 --from-beginning
    final static String outputTopic = "lzy-topic2";
    final static String jobTitle = "WordCount";

    public static void main(String[] args) throws Exception {
        System.setProperty("java.security.auth.login.config", "/usr/hdp/current/kafka-broker/conf/kafka_client_jaas.conf");
        final String bootstrapServers = args.length > 0 ? args[0] : "192.168.0.163:9092,192.168.0.164:9092,192.168.0.165:9092";

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //增加Kerberos认证
        Properties properties = new Properties();
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "GSSAPI");
        properties.setProperty("sasl.kerberos.service.name", "kafka");
        //消费者
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        KafkaRecordSerializationSchema<String> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic(outputTopic)
                .build();
        //生产者
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(properties)
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(serializer)
                .build();

        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        // Split up the lines in pairs (2-tuples) containing: (word,1)
        DataStream<String> counts = text.flatMap(new Tokenizer())
                // Group by the tuple field "0" and sum up tuple field "1"
                .keyBy(value -> value.f0)
                .sum(1)
                .flatMap(new Reducer());

        // Add the sink to so results
        // are written to the outputTopic
        counts.sinkTo(sink);

        // Execute program
        env.execute(jobTitle);
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // Emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    // Implements a simple reducer using FlatMap to
    // reduce the Tuple2 into a single string for
    // writing to kafka topics
    public static final class Reducer
            implements FlatMapFunction<Tuple2<String, Integer>, String> {

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<String> out) {
            // Convert the pairs to a string
            // for easy writing to Kafka Topic
            String count = value.f0 + " " + value.f1;
            out.collect(count);
        }
    }
}
