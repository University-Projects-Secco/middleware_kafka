# middleware_kafka

## Processing pipeline in Kafka

Implement a data processing pipeline in Kafka.

### Requirements

<ul>
    <li>Provide administrative tools / scripts to create and deploy a processing pipeline that 
        processes messages from a given topic.</li>
    <li>A processing pipeline consists of multiple stages, each of them processing an input 
        message at a time and producing one output message for the downstream stage.</li>
    <li>Different processing stages could run on different processes for scalability.</li>
    <li>Messages have a key, and the processing of messages with different keys is independent.
        <ul>
            <li>Stages are stateful and their state is partitioned by key 
                (where is the state stored?).</li>
            <li>Each stage consists of multiple processes that handle messages with different 
            keys in parallel.</li>
        </ul>
    </li>
    <li>Messages having the same key are processed in FIFO order with end-to-end exactly once
        delivery semantics.</li>
</ul>

### Assumptions

<ul>
    <li>Processes can fail.</li>
    <li>Kafka topics with replication factor > 1 can be considered reliable.</li>
    <li>You are only allowed to use Kafka Producers and Consumers API
        <ul>
            <li>You cannot use Kafka Processors or Streams API, but you can take inspiration 
                from their model.</li>
        </ul>
    </li>
    <li>You can assume a set of predefined functions to implement stages, and you can refer to 
        them by name in the scripts that create and deploy a processing pipeline.</li>
</ul>


### TODO
<ul>
    
</ul>

### TOTEST
<ul>
    <li>Kafka properties for producers and consumers</li>
    <li>Exactly once delivery</li>
    <li>Process fault</li>
</ul>