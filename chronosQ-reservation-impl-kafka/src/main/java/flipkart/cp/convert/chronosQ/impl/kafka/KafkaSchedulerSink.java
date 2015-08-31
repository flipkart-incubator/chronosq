package flipkart.cp.convert.chronosQ.impl.kafka;


import flipkart.cp.convert.chronosQ.core.SchedulerSink;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;



public class KafkaSchedulerSink implements SchedulerSink {

    static Logger log = LoggerFactory.getLogger(KafkaSchedulerSink.class.getSimpleName());
    private final String topic;
    private final KafkaMessage kafkaMessage;
    private final Producer producer;


    public KafkaSchedulerSink(ProducerConfig config,String topic, KafkaMessage kafkaMessage) {
        this.topic = topic;
        this.producer = new Producer(config);
        this.kafkaMessage =  kafkaMessage;
    }
    
    @Override
    public void giveExpiredForProcessing(String value) throws SchedulerException {
        if(null != value) {
            try {
                producer.send(kafkaMessage.getKeyedMessage(topic,value));
                log.info("Message published -" + value);
            } catch (Exception e) {
                log.error("Exception occurred for value " + value + "-" + e.fillInStackTrace());
                throw new SchedulerException(e, ErrorCode.SCHEDULER_SINK_ERROR);
            }
        }
    }

    @Override
    public void giveExpiredListForProcessing(List<String> values) throws SchedulerException {

        List <KeyedMessage<String,String>> data  = new ArrayList<KeyedMessage<String,String>>();
            for(String value :values){
                data.add(kafkaMessage.getKeyedMessage(topic, value));
            }
        try {
            producer.send(data);
            log.info("Message published -" +values);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Exception occurred for value " + values + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.SCHEDULER_SINK_ERROR);
        }
    }

}
