package flipkart.cp.convert.reservation.scheduler.worker.task;

import com.codahale.metrics.MetricRegistry;
import flipkart.cp.convert.ha.worker.task.WorkerTaskFactory;
import flipkart.cp.convert.chronosQ.core.SchedulerCheckpointer;
import flipkart.cp.convert.chronosQ.core.SchedulerSink;
import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.core.TimeBucket;

/**
 * Created by pradeep on 27/02/15.
 */
public class Factory implements WorkerTaskFactory {

    private final SchedulerCheckpointer checkpointer;
    private final SchedulerSink schedulerSink;
    private final SchedulerStore schedulerStore;
    private final TimeBucket timeBucket;
    private final MetricRegistry metricRegistry;
    private final int batchSize;

    public Factory(SchedulerCheckpointer checkpointer, SchedulerStore schedulerStore, TimeBucket timeBucket, SchedulerSink schedulerSink,MetricRegistry metricRegistry,int batchSize) {
        this.checkpointer = checkpointer;
        this.schedulerSink = schedulerSink;
        this.schedulerStore = schedulerStore;
        this.timeBucket = timeBucket;
        this.metricRegistry=metricRegistry;
        this.batchSize=batchSize;
    }

    @Override
    public WorkerTask getTask(String taskName) {
        return new WorkerTaskImpl(checkpointer, schedulerStore, timeBucket, schedulerSink, taskName,metricRegistry,batchSize);
    }
}