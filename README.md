## Overview
This is a sample project to observe issue with not committing offsets using `commitOffsetsInFinalize` for all Apache Kafka topic partitions when running on Direct Runner.

## Description

Please navigate to [test](src/test/java/com/marknorkin/beam/directrunner/sample/ParserEndToEndFlowCommitOffsetsTest.java) and execute present test case.

In this test scenario several types of messages (`known`, `partially known`, `unknown`) are sent to input topic 
named `raw_topic`, which contains 3 partitions. 
Then Apache Beam pipeline [determines](src/main/java/com/marknorkin/beam/directrunner/sample/transform) 
which type of message it received and outputs to one of the 3 output topics 
(`parsed_topic` - known, `partially_parsed_topic` - partially known, `unknown_messages_topic` - unknowns)

The result of test execution is **_not_** deterministic, when failing the exemplary stacktrace is as follows:
```
org.awaitility.core.ConditionTimeoutException: Condition with alias 'sent raw messages are read and offsets are committed' didn't complete within 3 minutes because lambda expression in com.marknorkin.beam.directrunner.sample.ParserEndToEndFlowCommitOffsetsTest: expected <{raw_topic-0=10, raw_topic-1=10, raw_topic-2=10}> but was <{raw_topic-1=10, raw_topic-0=10}>.

	at org.awaitility.core.ConditionAwaiter.await(ConditionAwaiter.java:145)
	at org.awaitility.core.AbstractHamcrestCondition.await(AbstractHamcrestCondition.java:89)
	at org.awaitility.core.ConditionFactory.until(ConditionFactory.java:902)
	at org.awaitility.core.ConditionFactory.until(ConditionFactory.java:645)
	at com.marknorkin.beam.directrunner.sample.ParserEndToEndFlowCommitOffsetsTest.shouldTestOffsetCommit(ParserEndToEndFlowCommitOffsetsTest.java:152)
```

For configuration of KafkaIO, please navigate [here](src/main/java/com/marknorkin/beam/directrunner/sample/KafkaIOConfig.java).

Expected result: finalization for all checkpoints of Apache Kafka topic partitions offsets are committed.