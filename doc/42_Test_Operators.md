# Test Operators

## DataStream Transformations

- map, yuanyangwu.flink.training.streaming.operator.MapTest
- flatMap, yuanyangwu.flink.training.streaming.operator.FlatMapTest
- filter, yuanyangwu.flink.training.streaming.operator.FilterTest
- keyBy, yuanyangwu.flink.training.streaming.operator.KeyByTest
  - keyByTupleTest, specify Tuple field by 0-based index
  - keyByTupleBasedPersonIncomingTest, specify Tuple-based getter/setting class field by 0-based index
  - keyByPojoTest, specify Pojo class field by string-type field name
- reduce, yuanyangwu.flink.training.streaming.operator.ReduceTest
- aggregations, yuanyangwu.flink.training.streaming.operator.AggregationTest
- window
- windowAll
- window apply
- window reduce
- window fold
- aggregations on windows
- union
- window join
- interval join
- window coGroup
- connect
- coMap, coFlapMap
- split
- select
- iterate
- extract timestamps

## Physical partitioning

## Task chaining and resource groups
