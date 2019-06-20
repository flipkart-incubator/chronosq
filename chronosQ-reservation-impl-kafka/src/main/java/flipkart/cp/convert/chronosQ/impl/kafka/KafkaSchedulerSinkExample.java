package flipkart.cp.convert.chronosQ.impl.kafka;

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by tushar.mandar on 2/26/15.
 */
public class KafkaSchedulerSinkExample {
    public static void main(String args[]) throws SchedulerException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "tgstage-bro-app-0002.ch.flipkart.com:9092");
        props.put("producer.type", "sync");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        KafkaSchedulerSink kafkaSchedulerSink = new KafkaSchedulerSink(props, "test_scheduler_002", new KafkaMessage() {
            @Override
            public ProducerRecord<byte[], byte[]> getKeyedMessage(String topic, String value) {
                return new ProducerRecord<byte[], byte[]>(topic, (value+value).getBytes() , value.getBytes());
            }
        });
        List<String> values = new ArrayList<String>();
        values.add("entry1");
        values.add("entry2");
        kafkaSchedulerSink.giveExpiredListForProcessing(values);
        kafkaSchedulerSink.giveExpiredForProcessing("entry3");

        kafkaSchedulerSink.giveExpiredListForProcessing(values);
        kafkaSchedulerSink.giveExpiredForProcessing("entry4");
    }
}
