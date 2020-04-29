package flipkart.cp.convert.chronosQ.impl.kafka;

import flipkart.cp.convert.chronosQ.core.DefaultSchedulerEntry;
import flipkart.cp.convert.chronosQ.core.SchedulerEntry;
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
            public ProducerRecord<byte[], byte[]> getKeyedMessage(String topic, SchedulerEntry schedulerEntry) {
                return new ProducerRecord<byte[], byte[]>(topic, schedulerEntry.getKey().getBytes(), schedulerEntry.getPayload().getBytes());
            }
        });
        List<SchedulerEntry> values = new ArrayList<>();
        values.add(new DefaultSchedulerEntry("entry1", "entry1"));
        values.add(new DefaultSchedulerEntry("entry2", "entry2"));
        kafkaSchedulerSink.giveExpiredListForProcessing(values);
        kafkaSchedulerSink.giveExpiredForProcessing(new DefaultSchedulerEntry("entry3", "entry3"));

        kafkaSchedulerSink.giveExpiredListForProcessing(values);
        kafkaSchedulerSink.giveExpiredForProcessing(new DefaultSchedulerEntry("entry4", "entry4"));
    }
}
