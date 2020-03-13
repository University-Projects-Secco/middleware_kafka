# middleware_kafka

##Processing pipeline in Kafka

Implement a data processing pipeline in Kafka.

###Requirements

• Provide administrative tools / scripts to create and deploy a processing pipeline that processes
messages from a given topic.
• A processing pipeline consists of multiple stages, each of them processing an input message
at a time and producing one output message for the downstream stage.
• Different processing stages could run on different processes for scalability.
• Messages have a key, and the processing of messages with different keys is independent.
o Stages are stateful and their state is partitioned by key (where is the state stored?).
o Each stage consists of multiple processes that handle messages with different keys in
parallel.
• Messages having the same key are processed in FIFO order with end-to-end exactly once
delivery semantics.

###Assumptions

• Processes can fail.
• Kafka topics with replication factor > 1 can be considered reliable.
• You are only allowed to use Kafka Producers and Consumers API
o You cannot use Kafka Processors or Streams API, but you can take inspiration from
their model.
• You can assume a set of predefined functions to implement stages, and you can refer to them
by name in the scripts that create and deploy a processing pipeline.
