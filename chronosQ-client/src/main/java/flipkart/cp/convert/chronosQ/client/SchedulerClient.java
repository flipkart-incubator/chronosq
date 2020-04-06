package flipkart.cp.convert.chronosQ.client;


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import flipkart.cp.convert.chronosQ.core.*;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 28/01/15
 * Time: 3:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class SchedulerClient<Entry extends SchedulerEntry> {

    private final SchedulerStore store;
    private final TimeBucket timeBucket;
    private final Partitioner partitioner;
    private final long DATASTORE_NO_OPERATION = 0;
    private final MetricRegistry metricRegistry;
    private Timer addTimeTaken;
    private Timer removeTimeTaken;
    private Timer updateTimeTaken;
    private static Logger log = LoggerFactory.getLogger(SchedulerClient.class.getName());

    public SchedulerClient(SchedulerStore store, TimeBucket timeBucket, Partitioner partitioner, MetricRegistry metricRegistry) {
        this.store = store;
        this.timeBucket = timeBucket;
        this.partitioner = partitioner;
        this.metricRegistry = metricRegistry;
    }

    public void add(Entry schedulerEntry, long time) throws SchedulerException {
        String val = schedulerEntry.getKey();
        int partitionNumber = partitioner.getPartition(val);
        Timer.Context context = null;
        if (metricRegistry != null) {
            addTimeTaken = metricRegistry.timer("scheduler.SchedulerClient.AddRequests" + "-Partition " + partitionNumber);
            context = addTimeTaken.time();
        }
        try {
            store.add(schedulerEntry, timeBucket.toBucket(time), partitionNumber);
            log.info("Add :" + val + "-" + time);
        } catch (Exception ex) {
            log.error("Unable to add to scheduler  :" + val + "-" + time + ex.fillInStackTrace());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((context != null))
                context.stop();
        }

    }

    public boolean update(Entry schedulerEntry, long oldTime, long newTime) throws SchedulerException {
        String val = schedulerEntry.getKey();
        boolean result = false;
        int partitionNumber = partitioner.getPartition(val);
        Timer.Context context = null;
        if (metricRegistry != null) {
            updateTimeTaken = metricRegistry.timer("scheduler.SchedulerClient.UpdateRequests" + "-Partition " + partitionNumber);
            context = updateTimeTaken.time();
        }
        try {
            Long storeResult = store.update(schedulerEntry, timeBucket.toBucket(oldTime), timeBucket.toBucket(newTime), partitionNumber);
            if (storeResult != DATASTORE_NO_OPERATION)
                result = true;
            log.info("Updated :" + val + "-" + "From " + oldTime + " To" + newTime);
            return result;
        } catch (Exception ex) {
            log.error("Unable to update value :" + val + "-" + "From " + oldTime + " To" + newTime + ex.fillInStackTrace());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((context != null))
                context.stop();
        }
    }

    public boolean remove(Entry schedulerEntry, long time) throws SchedulerException {
        String val = schedulerEntry.getKey();
        boolean result = false;
        int partitionNumber = partitioner.getPartition(val);
        Timer.Context context = null;
        if (metricRegistry != null) {
            removeTimeTaken = metricRegistry.timer("scheduler.SchedulerClient.RemoveRequests" + "-Partition " + partitionNumber);
            context = removeTimeTaken.time();
        }
        try {
            Long storeResult = store.remove(val, timeBucket.toBucket(time), partitionNumber);
            if (storeResult != DATASTORE_NO_OPERATION)
                result = true;
            log.info("Removed:" + val + "-" + time);
            return result;
        } catch (Exception ex) {
            log.error("Unable to remove from scheduler :" + val + "-" + time + ex.fillInStackTrace());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((context != null))
                context.stop();
        }
    }

    public static <Entry extends SchedulerEntry> SchedulerClient.Builder<Entry> builder() {
        return new SchedulerClient.Builder<>();
    }

    public static class Builder<Entry extends SchedulerEntry> {
        private SchedulerStore store;
        private TimeBucket timeBucket;
        private Partitioner partitioner;
        private MetricRegistry metricRegistry = null;
        private SchedulerClient<Entry> _client = null;

        public SchedulerClient<Entry> buildOrGet() {
            if (_client == null) {
                synchronized (this) {
                    if (_client == null) {
                        _client = new SchedulerClient<Entry>(store, timeBucket, partitioner, metricRegistry);
                    }
                }
            }
            return _client;
        }

        public Builder<Entry> withStore(SchedulerStore store) {
            this.store = store;
            return this;
        }

        public Builder<Entry> withTimeBucket(TimeBucket timeBucket) {
            this.timeBucket = timeBucket;
            return this;
        }

        public Builder<Entry> withPartitioner(Partitioner partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        public Builder<Entry> withMetricRegistry(MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
            return this;
        }
    }

}

