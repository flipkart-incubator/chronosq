package flipkart.cp.convert.chronosQ.impl.kafka;


import flipkart.cp.convert.chronosQ.core.SchedulerSink;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;


@Slf4j
public class KafkaSchedulerSink implements SchedulerSink {

    private final String topic;
    private final KafkaMessage kafkaMessage;
    private Producer<byte[], byte[]> producer;

    public KafkaSchedulerSink(Properties properties, String topic, KafkaMessage kafkaMessage) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(properties);
        this.kafkaMessage = kafkaMessage;
    }

    @Override
    public CompletableFuture<RecordMetadata> giveExpiredForProcessing(String value) throws SchedulerException {
        CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
        if (null != value) {
            try {
                log.debug("Pushing to kafka message: {}", value);
                producer.send(kafkaMessage.getKeyedMessage(topic, value), (metadata, exception) -> {
                    if(exception != null)
                        completableFuture.completeExceptionally(exception);
                    else
                        completableFuture.complete(metadata);
                });
                return completableFuture;
            } catch (Exception e) {
                log.error("Exception occurred for value " + value + "-" + e.fillInStackTrace());
                throw new SchedulerException(e, ErrorCode.SCHEDULER_SINK_ERROR);
            }
        }
        completableFuture.completeExceptionally(new Exception("Value is null"));
        return completableFuture;
    }

    @Override
    public Future<List<RecordMetadata>> giveExpiredListForProcessing(List<String> storeEntries) throws SchedulerException {
        List<ProducerRecord<byte[], byte[]>> data = new ArrayList<>();
        for (String storeValue : storeEntries) {
            if (null != storeValue) {
                data.add(kafkaMessage.getKeyedMessage(topic, storeValue));
            }
        }
        try {
            List<CompletableFuture<RecordMetadata>> results = data.stream().map(elem -> {
                CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
                producer.send(elem, (metadata, exception) -> {
                    if(exception != null)
                        completableFuture.completeExceptionally(exception);
                    else
                        completableFuture.complete(metadata);
                });
                return completableFuture;
            }).collect(Collectors.toList());
            return sequence(results);
        } catch (Throwable t) {
            log.error("Exception in {}, giveExpiredListForProcessing", t.getClass().getSimpleName());
            throw new SchedulerException(t, ErrorCode.SCHEDULER_SINK_ERROR);
        }
    }

    static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> com) {
        return CompletableFuture.allOf(com.toArray(new CompletableFuture<?>[com.size()]))
                .thenApply(v -> com.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                );
    }

}