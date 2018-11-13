# Run application on Flink cluster


## Build Flink application jar

Run "mvn clean package" to create jar at target/flink-training-0.1.jar
pom.xml makes jar include all dependencies except Flink runtime, which is available in Flink cluster.

## Launch Kafka and Flink cluster

Check out my Kafka vagrant VM, power it up, and then log on it

```bash
git clone https://github.com/yuanyangwu/vmlab.git
cd kafka
vagrant up
vagrant ssh
```

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

- Start Flink cluster

```console
cd /home/vagrant/flink/single/server0/flink-1.6.2
sudo bin/start-cluster.sh
```

## Manage Flink jobs via Flink web server

### Open Flink web page in web browser

http://192.168.11.60:8081/

### Submit job with default settings

1. On web page "Submit new Job"
  a. Click "Add New" button to upload flink-training-0.1.jar
  b. Verify jar is in "Uploaded Jars" list
  c. Check flink-training-0.1.jar, keep input empty and click "Submit" button
2. On web page "Completed Jobs", job name "Flink Streaming Java API Skeleton" is listed
3. On SSH terminal, check task manager out

```console
vagrant@server01:~/flink/single/server0/flink-1.6.2/log$ cat flink-root-taskexecutor-0-server01.out
1
5
10
15
```

Where does Job Name come from?

pom.xml has "<mainClass>yuanyangwu.flink.training.StreamingJob</mainClass>". That means Flink will get started with "StreamingJob.main()".
The job name is set by ```env.execute("Flink Streaming Java API Skeleton");``` in yuanyangwu.flink.training.StreamingJob.main().

### Submit job with specified main class

flink-training-0.1.jar has multiple main class. We have 2 ways to specify another main class

- Option 1: Modify pom.xml's mainClass, build and upload a new jar
- Option 2: Submit jar with specified main class on web page

Here are option 2's steps.

1. On web page "Submit new Job"
  a. Check flink-training-0.1.jar\
  b. Set "Entry Class" = "yuanyangwu.flink.training.streaming.source.TimestampedSourceApp"
  c. Click "Submit" button
2. On web page "Running Jobs", job name "TimestampedSourceApp" is listed
3. On SSH terminal, check task manager log

```console
vagrant@server01:~/flink/single/server0/flink-1.6.2/log$ tail -f flink-root-taskexecutor-0-server01.log
2018-11-13 07:30:35,586 INFO  yuanyangwu.flink.training.util.LogSink                        - sink1 timestamp=2018-11-13T07:30:35.586 watermark=2018-11-13T07:30:35.082 value=(2018-11-13T07:30:35.586,45)
2018-11-13 07:30:35,686 INFO  yuanyangwu.flink.training.util.LogSink                        - sink1 timestamp=2018-11-13T07:30:35.686 watermark=2018-11-13T07:30:35.082 value=(2018-11-13T07:30:35.686,46)
```

4. On "Running Jobs"
  a. Click job "TimestampedSourceApp" to show job details
  b. Click "Cancel" button to cancel the job
5. On "Completed Jobs", job "TimestampedSourceApp" is listed after a while

### Submit Kafka sink and source jobs

Default Flink task slot number is 1. When a job is running, the 2nd job will keep in "Created" state until previous job exits.

We need increase slot number to run 2 tasks in parallel.

- Change "taskmanager.numberOfTaskSlots" from 1 to 4 in /home/vagrant/flink/single/server0/flink-1.6.2/conf/flink-conf.yaml
- Restart Flink cluster via "sudo bin/stop-cluster.sh && sudo bin/start-cluster.sh"
- On web page "Overview", "Task Slots" is 4 now.

Run 2 Kafka jobs

1. Submit Kafka sink on web page "Submit new Job"
  a. Check flink-training-0.1.jar\
  b. Set "Entry Class" = "yuanyangwu.flink.training.streaming.source.KafkaSinkApp"
  c. Click "Submit" button
2. Submit Kafka source on web page "Submit new Job"
  a. Check flink-training-0.1.jar\
  b. Set "Entry Class" = "yuanyangwu.flink.training.streaming.source.KafkaSourceApp"
  c. Click "Submit" button
3. On web page "Running Jobs", jobs "KafkaSinkApp" and "KafkaSourceApp" are listed
4. On SSH terminal, check task manager log
  - After KafkaSink writes an event, KafkaSource reads the same event.
  - For same event, "timestamp=..." shows event timestamps are same in KafkaSink and KafkaSource because event timestamp is included in comma-separated event
  - For same event, "watermark=..." shows watermarks in KafkaSink and KafkaSource are different because watermark is determined by application itself.

```console
vagrant@server01:~/flink/single/server0/flink-1.6.2/log$ tail -f flink-root-taskexecutor-0-server01.log
2018-11-13 13:54:05,523 INFO  yuanyangwu.flink.training.util.LogSink                        - KafkaSink timestamp=2018-11-13T13:54:05.523 watermark=2018-11-13T13:54:04.605 value=2018-11-13T13:54:05.523,19
2018-11-13 13:54:05,542 INFO  yuanyangwu.flink.training.util.LogSink                        - KafkaSource timestamp=2018-11-13T13:54:05.523 watermark=2018-11-13T13:54:05.420 value=2018-11-13T13:54:05.523,19
2018-11-13 13:54:05,625 INFO  yuanyangwu.flink.training.util.LogSink                        - KafkaSink timestamp=2018-11-13T13:54:05.624 watermark=2018-11-13T13:54:04.605 value=2018-11-13T13:54:05.624,20
2018-11-13 13:54:05,646 INFO  yuanyangwu.flink.training.util.LogSink                        - KafkaSource timestamp=2018-11-13T13:54:05.624 watermark=2018-11-13T13:54:05.522 value=2018-11-13T13:54:05.624,20
```
