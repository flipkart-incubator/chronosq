package flipkart.cp.convert.chronosQ.impl.kafka;


import kafka.producer.KeyedMessage;

public interface KafkaMessage {
    KeyedMessage<String, String> getKeyedMessage(String topic , String value);
}
