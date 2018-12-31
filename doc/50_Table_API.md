# Table API

## Table pom dependency

To use Flink table API, following depenencies must be added to pom.xml

```text
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_${scala.binary.version}</artifactId>
    <version>${flink.version}</version>
    <scope>provided</scope>
</dependency>
```