# Test Flink Application with JUnit

## Add test dependencies to pom.xml

scope "test" means the dependency is only used in test.

```xml
<dependencies>
    <dependency>
        <groupId>junit</groupId>
        <artifactId>junit</artifactId>
        <version>4.12</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-test-utils_${scala.binary.version}</artifactId>
        <version>${flink.version}</version>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## Add test directories in IntelliJ IDEA

- Right click "src" directory and select New > Directory to create "test" directory
- Right click "test" directory and select New > Directory to create "java" directory
- Right click "test/java" directory and select Make Directory as > Test Sources Root

## Add a test case

- Open yuanyangwu.flink.training.util.FlinkTimestamp file
- In editor, select FlinkTimestamp, enter Alt+Enter, and select Create Test. Test class yuanyangwu.flink.training.util.FlinkTimestampTest is generated under test/java
- Add a test case

```java
public class FlinkTimestampTest {
    @Test
    public void fromLocalDateTime() {
        LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
        long epoch = FlinkTimestamp.fromLocalDateTime(now);
        assertEquals(now, FlinkTimestamp.toLocalDateTime(epoch));
    }
}
```
