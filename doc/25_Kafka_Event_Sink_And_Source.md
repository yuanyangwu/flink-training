# Kafka Event Sink and Source

Flink uses "connector" to interact with external event source and sink like Kafka. We will do followings

- Create one application connecting a Kafka sink, which writes events to Kafka
  - Use TimestampedSourceApp.TimestampedIntegerSource as Flink source. It generates events every 100 ms.
  - Convert tuple event to comma-separated string
  - Write string-type event to Kafka
- Create another application connecting a Kafka source, which reads events from Kafka
- Use LogSink to monitor events written to and read from Kafka

class:

- yuanyangwu.flink.training.streaming.source.KafkaSinkApp
- yuanyangwu.flink.training.streaming.source.KafkaSourceApp

## Add Kafka connector to pom.xml

```text
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka-0.11_2.11</artifactId>
    <version>${flink.version}</version>
</dependency>
```

## Kafka Sink

```java
public class KafkaSinkApp {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<String>(
                    "192.168.11.60:9092", // Kafka broker
                    "my_topic", // Kafka topic
                    new SimpleStringSchema()); // String-type event

            // When writing events to Kakfa, write each event with its event timestamp.
            // Kafka uses event timestamp for retention
            // Unfortunately, Kafka consumer cannot print Kafka event timestamp.
            producer.setWriteTimestampToKafka(true);

            // Add TimestampedSourceApp.TimestampedIntegerSource as source
            // then convert it to comma-separated string
            SingleOutputStreamOperator<String> source = env
                    .addSource(new TimestampedSourceApp.TimestampedIntegerSource())
                    .map(new MapFunction<Tuple2<LocalDateTime, Integer>, String>() {
                        @Override
                        public String map(Tuple2<LocalDateTime, Integer> value) throws Exception {
                            return value.f0.toString() + "," + value.f1;
                        }
                    });

            // connect same source to both Kafka sink and LogSink sink to monitor what is written to Kafka
            source.addSink(producer);
            source.addSink(new LogSink<String>("KafkaSink"));

            env.execute("KafkaSinkApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
```

## Kafka Source

```java
public class KafkaSourceApp {
    public static void main(String[] args) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env
                    .setParallelism(1)
                    .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "192.168.11.60:9092");
            // only required for Kafka 0.8
            properties.setProperty("zookeeper.connect", "192.168.11.60:2181");
            properties.setProperty("group.id", "kafka-source");

            FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
                    "my_topic", // Kafka topic
                    new SimpleStringSchema(), // String-type event
                    properties); // Kafka properties
            consumer
                    .setStartFromEarliest() // read from beginning
                    .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<String>() { // parse timestamp from event
                        private static final long serialVersionUID = 1L;
                        private long timestamp = FlinkTimestamp.fromLocalDateTime(LocalDateTime.now());

                        @Override
                        public long extractTimestamp(String element, long previousElementTimestamp) {
                            try {
                                // parse date time from first field
                                String [] fields = element.split(",", 2);
                                LocalDateTime dateTime = LocalDateTime.parse(fields[0]);
                                timestamp = FlinkTimestamp.fromLocalDateTime(dateTime);
                            } catch (DateTimeParseException e) {
                                // increase by 1 if first field is invalid
                                timestamp++;
                            }
                            return timestamp;
                        }

                        @Override
                        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
                            return new Watermark(extractedTimestamp - 1);
                        }

                    });

            DataStreamSource<String> source = env.addSource(consumer);
            source
                    .addSink(new LogSink<String>("KafkaSource"));

            env.execute("KafkaSourceApp");
        } catch (Exception e) {
            LOG.error("main", e);
        }
    }
}
```

## Launch Kafka Cluster

Check out my Kafka vagrant VM, power it up, and then log on it

```bash
git clone https://github.com/yuanyangwu/vmlab.git
cd kafka
vagrant up
vagrant ssh
```

A usage document is at "/home/vagrant/usage.txt". Run multiple commands in different terminals. You can either use"screen" or open new terminals via "vagrant ssh".

- Start Zookeeper

```console
cd /home/vagrant/kafka/single/server0/kafka_2.11-2.0.0
sudo bin/zookeeper-server-start.sh config/zookeeper.properties
```

- Start Kafka server

```console
cd /home/vagrant/kafka/single/server0/kafka_2.11-2.0.0
sudo bin/kafka-server-start.sh config/server.properties
```

- Create a topic "my_topic"

```console
vagrant@server01:~/kafka/single/server0/kafka_2.11-2.0.0$ sudo bin/kafka-topics.sh --create --topic my_topic --zookeeper 192.168.11.60:2181 --replication-factor 1 --partitions 1
WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
Created topic "my_topic".
vagrant@server01:~/kafka/single/server0/kafka_2.11-2.0.0$ sudo bin/kafka-topics.sh --describe --topic my_topic --zookeeper 192.168.11.60:2181
Topic:my_topic  PartitionCount:1        ReplicationFactor:1     Configs:
        Topic: my_topic Partition: 0    Leader: 0       Replicas: 0     Isr: 0
vagrant@server01:~/kafka/single/server0/kafka_2.11-2.0.0$
```

- In IntelliJ IDEA, run KafkaSinkApp.main and then run KafkaSourceApp.main, wait a while, and then stop KafkaSinkApp.main and then stop KafkaSourceApp.main
  - KafkaSink log
    - "timestamp=..." shows TimestampedSourceApp.TimestampedIntegerSource generates events every 100ms
    - "value=..." shows that Tuple2<LocalDateTime, Integer> is mapped to a comma-separated string of timestamp and integer
  - KafkaSource log
    - "timestamp=..." shows event timestamp is same as KafkaSink's. Even if log time is different, event timestamp is kept because it parses timestamp from "value=..."
    - "value=..." is same as KafkaSink's

```console
21:37:26,603 INFO  yuanyangwu.flink.training.util.LogSink                        - KafkaSink timestamp=2018-11-12T13:37:26.603 watermark=2018-11-12T13:37:26.201 value=2018-11-12T13:37:26.603,44
21:37:26,703 INFO  yuanyangwu.flink.training.util.LogSink                        - KafkaSink timestamp=2018-11-12T13:37:26.703 watermark=2018-11-12T13:37:26.201 value=2018-11-12T13:37:26.703,45
```

```console
21:37:26,618 INFO  yuanyangwu.flink.training.util.LogSink                        - KafkaSource timestamp=2018-11-12T13:37:26.603 watermark=2018-11-12T13:37:26.502 value=2018-11-12T13:37:26.603,44
21:37:26,713 INFO  yuanyangwu.flink.training.util.LogSink                        - KafkaSource timestamp=2018-11-12T13:37:26.703 watermark=2018-11-12T13:37:26.602 value=2018-11-12T13:37:26.703,45
```
