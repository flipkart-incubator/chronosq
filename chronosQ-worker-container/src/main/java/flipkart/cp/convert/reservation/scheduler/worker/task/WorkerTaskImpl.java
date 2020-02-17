package flipkart.cp.convert.reservation.scheduler.worker.task;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import flipkart.cp.convert.chronosQ.core.*;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 17/02/15
 * Time: 10:56 AM
 * To change this template use File | Settings | File Templates.
 */

public class WorkerTaskImpl extends WorkerTask {
    private final SchedulerCheckpointer checkpointer;
    private final SchedulerStore schedulerStore;
    private final TimeBucket timeBucket;
    private final SchedulerSink schedulerSink;
    private final MetricRegistry metricRegistry;
    private final String metricGaugeName;
    private final Timer sinkPushingTime;
    private final int batchSize;
    private boolean interrupt = false;
    static Logger log = LoggerFactory.getLogger(WorkerTaskImpl.class.getName());

    public WorkerTaskImpl(SchedulerCheckpointer checkpointer, SchedulerStore schedulerStore, TimeBucket timeBucket, SchedulerSink schedulerSink, String taskName, MetricRegistry metricRegistry, int batchSize) {
        super(taskName);
        this.checkpointer = checkpointer;
        this.schedulerStore = schedulerStore;
        this.timeBucket = timeBucket;
        this.schedulerSink = schedulerSink;
        this.metricRegistry = metricRegistry;
        this.batchSize = batchSize;
        log.info("WorkerTaskImpl: " + taskName);
        sinkPushingTime = metricRegistry.timer("sinkPushingTime-Partition " + getPartitionNum());
        metricGaugeName = "ElapsedTimeWorkerToProcess TimeDiffInMilliSec -Partition " + getPartitionNum();
        try {
            metricRegistry.register(MetricRegistry.name(WorkerTaskImpl.class, metricGaugeName),
                    new Gauge<Long>() {
                        @Override
                        public Long getValue() {
                            try {
                                return getCurrentDateTimeInMilliSecs() - calculateNextIntervalForProcess(getPartitionNum());
                            } catch (SchedulerException e) {
                                log.error("Exception in initializing WorkerTaskImpl: ", e);
                            }
                            return Long.MIN_VALUE;
                        }
                    });
        } catch (IllegalArgumentException ex) {
            log.warn("Metrics already registered for this partition.Calling in restart gives failure.Ignoring");
        }

    }

    @Override
    public void process() {
        while (!interrupt && !Thread.currentThread().isInterrupted()) {
            try {
                long currentDateTimeInMilliSec = getCurrentDateTimeInMilliSecs();
                long nextIntervalForProcess = calculateNextIntervalForProcess(getPartitionNum());
                while (!interrupt && nextIntervalForProcess <= currentDateTimeInMilliSec) {
                    List<SchedulerData> values = schedulerStore.getNextN(nextIntervalForProcess, getPartitionNum(), batchSize);

                    while (!interrupt && !values.isEmpty()) {
                        final Timer.Context context = sinkPushingTime.time();
                        schedulerSink.giveExpiredListForProcessing(values);
                        context.stop();
                        schedulerStore.removeBulk(nextIntervalForProcess, getPartitionNum(), values.stream().map(SchedulerData::getKey).collect(Collectors.toList()));
                        values = schedulerStore.getNextN(nextIntervalForProcess, getPartitionNum(), batchSize);
                    }
                    checkpointer.set(String.valueOf(nextIntervalForProcess), getPartitionNum());
                    log.info("Processed for " + nextIntervalForProcess + " in partition " + getPartitionNum());
                    nextIntervalForProcess = timeBucket.next(nextIntervalForProcess);
                    currentDateTimeInMilliSec = getCurrentDateTimeInMilliSecs();
                }
                // Sleeping if currently got processed
                log.info("sleep for " + (nextIntervalForProcess - currentDateTimeInMilliSec));
                Thread.sleep(nextIntervalForProcess - currentDateTimeInMilliSec);
            } catch (InterruptedException e) {
                log.error("Thread interrupted. Breaking and Restarting. ", e);
                break;
            } catch (Exception ex) {
                log.error("Exception in processing WorkerTaskImpl. Restarting ", ex);
            }
        }

    }

    public void interrupt() {
        this.interrupt = true;
    }

    @Override
    public void stopGraceFully() {
        interrupt();
    }

    @Override
    public void stopPoisonPill() {
        //InterruptedException can be swallowed by enclosing code - child thread isnt guaranteed to be killed.
        Thread.currentThread().stop();
    }

    private long calculateNextIntervalForProcess(int partitionNum) throws SchedulerException {

        long timerKeyConverted;
        try {
            String timerKey = checkpointer.peek(partitionNum);
            timerKeyConverted = Long.valueOf(timerKey);

        } catch (Exception ex) {
            timerKeyConverted = new Date().getTime() - 1000 * 10;
            log.error("Checkpointer key is null, Creating key from current time -" + timerKeyConverted);
            checkpointer.set(String.valueOf(timerKeyConverted), partitionNum);
        }
        return timeBucket.toBucket(timerKeyConverted);
    }

    private long getCurrentDateTimeInMilliSecs() {
        return new Date().getTime();
    }


}
