package flipkart.cp.convert.chronosQ.impl.hbase;

import flipkart.cp.convert.chronosQ.core.DefaultSchedulerEntry;
import flipkart.cp.convert.chronosQ.core.SchedulerEntry;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by tushar.mandar on 2/26/15.
 */
public class HbaseSchedulerExample {
    public static void main(String args[]) throws SchedulerException {

        Configuration hConfig = HBaseConfiguration.create();
        hConfig.set(HConstants.ZOOKEEPER_QUORUM, "ce-sandbox-hbase-0001.nm.flipkart.com");
        hConfig.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        hConfig.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "60000");
        hConfig.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "60000");
        hConfig.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure");

        Connection hConnection = null;
        try {
            hConnection = ConnectionFactory.createConnection(hConfig);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assert (null != hConnection) : "hConnection is not initialized";

        String instanceName = "instance1";
        Long time = System.currentTimeMillis();

        //Scheduler Store
        HbaseSchedulerStore hbaseSchedulerStore = new HbaseSchedulerStore(hConnection, "connekt-scheduled-requests", "d", instanceName);
        hbaseSchedulerStore.add(new DefaultSchedulerEntry("entry1"), time, 1);
        hbaseSchedulerStore.remove("entry1", time, 1);

        hbaseSchedulerStore.add(new DefaultSchedulerEntry("entry2"), time, 2);
        hbaseSchedulerStore.add(new DefaultSchedulerEntry("entry3"), time, 2);
        hbaseSchedulerStore.add(new DefaultSchedulerEntry("entry4"), time, 2);

        List<SchedulerEntry> entries = hbaseSchedulerStore.get(time, 2);
        assert (entries.size() > 0) : String.format("No added entries found in scheduler store time - %d partition - %d", time, 2);
        for (SchedulerEntry entry : entries)
            System.out.println("GET Response: " + entry);

        entries = hbaseSchedulerStore.getNextN(time, 2, 2);
        assert (entries.size() > 0) : String.format("No added entries found in scheduler store time - %d partition - %d", time, 2);
        for (SchedulerEntry entry : entries)
            System.out.println("GET Next 2 Response: " + entry);

        hbaseSchedulerStore.removeBulk(time, 2, entries.stream().map(SchedulerEntry::getKey).collect(Collectors.toList()));
        entries = hbaseSchedulerStore.getNextN(time, 2, 2);
        System.out.println("GET Next 2 Response size after deleting 2: " + entries.size());
        hbaseSchedulerStore.removeBulk(time, 2, entries.stream().map(SchedulerEntry::getKey).collect(Collectors.toList()));

        entries = hbaseSchedulerStore.getNextN(time, 2, 2);
        System.out.println("GET Next 2 Response size after deleting all: " + entries.size());
        hbaseSchedulerStore.removeBulk(time, 2, entries.stream().map(SchedulerEntry::getKey).collect(Collectors.toList()));

        //scheduler check-pointer
        HbaseSchedulerCheckpoint hbaseSchedulerCheckpoint = new HbaseSchedulerCheckpoint(hConnection, "connekt-schedule-checkpoints", "d", instanceName);
        hbaseSchedulerCheckpoint.set(Long.toString(time), 2);
        String value = hbaseSchedulerCheckpoint.peek(2);
        System.out.println("peek value: " + value);
        hbaseSchedulerCheckpoint.set(Long.toString(time + 100L), 2);
    }
}
