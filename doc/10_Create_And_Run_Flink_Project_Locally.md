# Create and Run Flink Project Locally

## Generate a Java Flink Project via maven

Under Linux console, run following command

```console
mvn archetype:generate                               \
  -DarchetypeGroupId=org.apache.flink                \
  -DarchetypeArtifactId=flink-quickstart-java        \
  -DarchetypeVersion=1.6.1                           \
  -DgroupId=yuanyangwu.flink                         \
  -DartifactId=flink-training                        \
  -Dversion=0.1                                      \
  -Dpackage=yuanyangwu.flink.training                \
  -DinteractiveMode=false
```

Under Windows console, run following command

```console
mvn archetype:generate                               ^
  -DarchetypeGroupId=org.apache.flink                ^
  -DarchetypeArtifactId=flink-quickstart-java        ^
  -DarchetypeVersion=1.6.1                           ^
  -DgroupId=yuanyangwu.flink                         ^
  -DartifactId=flink-training                        ^
  -Dversion=0.1                                      ^
  -Dpackage=yuanyangwu.flink.training                ^
  -DinteractiveMode=false
```

Here is the generated project.

```console
│  pom.xml
│
└─src
    └─main
        ├─java
        │  └─yuanyangwu
        │      └─flink
        │          └─training
        │                  BatchJob.java
        │                  StreamingJob.java
        │
        └─resources
                log4j.properties
```

## Add a simple logic to class StreamingJob

```java
public class StreamingJob {
     public static void main(String[] args) {
          // set up the streaming execution environment
          final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

          env
                    .setParallelism(1)
                    .fromElements(1, 5, 10, 15)
                    .print();

          // execute program
          env.execute("Flink Streaming Java API Skeleton");
     }
}
```

## Run project in IDE

- Import the project into IntelliJ IDEA.
- Right click ```src/main/java/yuanyangwu/flink/training/StreamingJob``` and select menu "Run 'StreamingJob.main'"

```console
21:58:30,975 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: Collection Source -> Sink: Print to Std. Out (1/1) (64afe53b6f4ba6e01aac84b63ba8ff25) switched from DEPLOYING to RUNNING.
21:58:30,979 INFO  org.apache.flink.streaming.runtime.tasks.StreamTask           - No state backend has been configured, using default (Memory / JobManager) MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)
1
5
10
15
21:58:31,104 INFO  org.apache.flink.runtime.taskmanager.Task                     - Source: Collection Source -> Sink: Print to Std. Out (1/1) (64afe53b6f4ba6e01aac84b63ba8ff25) switched from RUNNING to FINISHED.
```

## Run project via maven in console

It fails.

```console
> mvn exec:java -Dexec.mainClass="yuanyangwu.flink.training.StreamingJob" -Dexec.cleanupDaemonThreads=false

java.lang.NoClassDefFoundError: org/apache/flink/streaming/api/environment/StreamExecutionEnvironment
        at yuanyangwu.flink.training.StreamingJob.main(StreamingJob.java:39)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.codehaus.mojo.exec.ExecJavaMojo$1.run(ExecJavaMojo.java:282)
        at java.lang.Thread.run(Thread.java:748)
Caused by: java.lang.ClassNotFoundException: org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
        at java.net.URLClassLoader.findClass(URLClassLoader.java:381)
        at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
        at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
        ... 7 more
```

The failure reason is that pom.xml marks flink dependency as "provided". That means flink jar files are only used in compile time. It is to make "mvn package" create a small package excluding unnecessary files.

```console
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-java</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
```

IntelliJ IDEA can run the project successfully because pom.xml has a special profile enabling related flink jar files.

```console
<profile>
    <id>add-dependencies-for-IDEA</id>

    <dependencies>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-java</artifactId>
            <version>${flink.version}</version>
            <scope>compile</scope>
        </dependency>
```

To run the project in console, we can enable same profile. It prints results successfully with error message.

```console
> mvn exec:java -Padd-dependencies-for-IDEA -Dexec.mainClass="yuanyangwu.flink.training.StreamingJob"

22:11:56,197 INFO  org.apache.flink.streaming.runtime.tasks.StreamTask           - No state backend has been configured, using default (Memory / JobManager) MemoryStateBackend (data in heap memory / checkpoints to JobManager) (checkpoints: 'null', savepoints: 'null', asynchronous: TRUE, maxStateSize: 5242880)
1
5
10
15
22:11:56,300 INFO  org.apache.flink.runtime.taskmanager.Task                     - Source: Collection Source -> Sink: Print to Std. Out (1/1) (b3260df6f854f492e5cffe475edcf6e2) switched from RUNNING to FINISHED.

22:11:56,488 INFO  org.apache.flink.runtime.rpc.akka.AkkaRpcService              - Stopped Akka RPC service.
[WARNING] thread Thread[ObjectCleanerThread,1,yuanyangwu.flink.training.StreamingJob] was interrupted but is still alive after waiting at least 14999msecs
[WARNING] thread Thread[ObjectCleanerThread,1,yuanyangwu.flink.training.StreamingJob] will linger despite being asked to die via interruption
[WARNING] thread Thread[FlinkCompletableFutureDelayScheduler-thread-1,5,yuanyangwu.flink.training.StreamingJob] will linger despite being asked to die via interruption
[WARNING] NOTE: 2 thread(s) did not finish despite being asked to  via interruption. This is not a problem with exec:java, it is a problem with the running code. Although not serious, it should be remedied.
[WARNING] Couldn't destroy threadgroup org.codehaus.mojo.exec.ExecJavaMojo$IsolatedThreadGroup[name=yuanyangwu.flink.training.StreamingJob,maxpri=10]
java.lang.IllegalThreadStateException
```

The error message can be resolved with "-Dexec.cleanupDaemonThreads=false". Here is the final maven command.

```console
mvn exec:java -Padd-dependencies-for-IDEA -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="yuanyangwu.flink.training.StreamingJob"
```
