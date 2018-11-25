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
- window, yuanyangwu.flink.training.streaming.operator.WindowTest
  - tumblingWindowTest, tumbling window
  - slideWindowTest, sliding window
  - sessionWindowTest, session window
- windowAll yuanyangwu.flink.training.streaming.operator.WindowAllTest
  - tumblingWindowAllTest, tumbling window all-keys
  - slideWindowAllTest, sliding window all-keys
  - sessionWindowAllTest, session window all-keys
- window apply yuanyangwu.flink.training.streaming.operator.WindowAllTest
- window reduce yuanyangwu.flink.training.streaming.operator.WindowReduceTest
- aggregations on windows yuanyangwu.flink.training.streaming.operator.WindowAggregationTest
- union yuanyangwu.flink.training.streaming.operator.UnionTest
- window join yuanyangwu.flink.training.streaming.operator.WindowJoinTest
- interval join yuanyangwu.flink.training.streaming.operator.IntervalJoinTest
- window coGroup yuanyangwu.flink.training.streaming.operator.WindowCoGroupTest
- connect, coMap, coFlapMap yuanyangwu.flink.training.streaming.operator.ConnectTest
- split, select yuanyangwu.flink.training.streaming.operator.SplitTest
- iterate
- extract timestamps

## Physical partitioning

## Task chaining and resource groups
