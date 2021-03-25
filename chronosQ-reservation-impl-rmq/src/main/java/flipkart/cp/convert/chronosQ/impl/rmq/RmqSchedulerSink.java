package flipkart.cp.convert.chronosQ.impl.rmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import flipkart.cp.convert.chronosQ.core.SchedulerEntry;
import flipkart.cp.convert.chronosQ.core.SchedulerSink;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class RmqSchedulerSink implements SchedulerSink {

    private Channel channel;
    private String exchange;
    private String queueName;
    private BasicProperties properties = null;

    public RmqSchedulerSink(Channel channel, String exchange, String queueName) {
        this.channel = channel;
        this.exchange = exchange;
        this.queueName = queueName;
    }

    public RmqSchedulerSink(Channel channel, String exchange) {
        this.channel = channel;
        this.exchange = exchange;
    }

    public RmqSchedulerSink(Channel channel, String exchange, String queueName, BasicProperties properties) {
        this(channel, exchange, queueName);
        this.properties = properties;
    }

    @Override
    public CompletableFuture<Void> giveExpiredForProcessing(SchedulerEntry schedulerEntry) throws SchedulerException {
        try {
            log.info("Got message to be published " + schedulerEntry);
            channel.basicPublish(exchange, queueName, null, schedulerEntry.getPayload().getBytes());
            log.info("Message published -" + schedulerEntry);
            return CompletableFuture.completedFuture(null);
        } catch (IOException ex) {
            log.error("Unable to publish message to queue - " + schedulerEntry + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.SCHEDULER_SINK_ERROR);
        }
    }

    @Override
    public CompletableFuture<Void> giveExpiredListForProcessing(List<SchedulerEntry> schedulerEntries) throws SchedulerException {
        for (SchedulerEntry value : schedulerEntries)
            giveExpiredForProcessing(value);
        return CompletableFuture.completedFuture(null);
    }
}
