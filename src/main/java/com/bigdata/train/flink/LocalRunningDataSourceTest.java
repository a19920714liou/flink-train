package com.bigdata.train.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.util.bash.FlinkConfigLoader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class LocalRunningDataSourceTest {
    public static void main(String[] args) {
        // 采用本地模式
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从本地的webUI方式提供createLocalEnvironmentWithWebUI
        /*Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);*/ //采用默认配置


        // 设定数据来源为集合数据
        DataStream<Person> flintstones = env.
                fromElements(new Person("Fred", 35),
                        new Person("Wilma", 35),
                        new Person("Pebbles", 2));

        DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });

        adults.print();


        // 设置数据来源为当前的文本文件：
        DataStreamSource<String> readTextFile = env.readTextFile("E:\\GitHub\\flink-train\\src\\main\\resources\\abc.txt");
        // 直接读取文件为文本类型，最后进行print操作
        readTextFile.print();

        try {
            // 最后开始执行
            JobExecutionResult result = env.execute("Fraud Detection");
            if (result.isJobExecutionResult()) {
                System.out.println("执行完毕......");
            }
            System.out.println();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static class Person {
        private String name;
        private Integer age;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
