package flipkart.cp.convert.chronosQ.impl.kafka;


import org.apache.kafka.clients.producer.ProducerRecord;

public interface KafkaMessage {
    ProducerRecord<byte[], byte[]> getKeyedMessage(String topic , String value);
}
