package flipkart.cp.convert.chronosQ.impl.hbase;

import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class HbaseSchedulerStore implements SchedulerStore {

    static Logger log = LoggerFactory.getLogger(HbaseSchedulerStore.class.getSimpleName());
    static private final String START_STRING = "0";
    static private final String END_STRING = "~";
    static private final String DELIMITER = "__";
    private final Connection hConnection;
    private final String tableName;
    private final byte[] columnFamily;
    private final byte[] column;
    private final String schedulerInstance;
    private Map<byte[], byte[]> dummyData = new HashMap<>();

    private HTable hTable;

    private ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2);
    private ScheduledFuture<?> flusher = executor.scheduleAtFixedRate(() -> {
        try {
            hTable.flushCommits();
        } catch (IOException e) {
            log.error("Exception occurred  for flushing to events hbase -"  + e.fillInStackTrace(), e);
        }
    }, 10, 10, TimeUnit.SECONDS);

    public HbaseSchedulerStore(Connection hConnection, String tableName, String columnFamily, String schedulerInstance) throws SchedulerException {
        this.hConnection = hConnection;
        this.tableName = tableName;
        this.schedulerInstance = schedulerInstance;
        this.columnFamily = Bytes.toBytes(columnFamily);
        this.column = Bytes.toBytes("d");
        dummyData.put(column, Bytes.toBytes(true));
        try {
            hTable = getHTable();
        } catch (IOException e) {
            throw new SchedulerException(e, ErrorCode.SCHEDULER_SINK_ERROR);
        }
    }


    @Override
    public void add(String value, long time, int partitionNo) throws SchedulerException {
        String rowKey = getRowKey(value, time, partitionNo);
        try {
            hTable.put(HbaseUtils.createPut(rowKey, columnFamily, dummyData));
        } catch (IOException e) {
            log.error("Exception occurred  for adding  -" + value + "Key" + time + "Partition " + partitionNo + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }

    @Override
    public Long update(String value, long oldTime, long newTime, int partitionNo) throws SchedulerException {
        add(value, newTime, partitionNo);
        return remove(value, oldTime, partitionNo);
    }

    @Override
    public Long remove(String value, long time, int partitionNo) throws SchedulerException {
        String rowKey = getRowKey(value, time, partitionNo);
        try {
            boolean deleted = hTable.checkAndDelete(Bytes.toBytes(rowKey), columnFamily, column, dummyData.get(column), new Delete(Bytes.toBytes(rowKey)));
            if (deleted)
                return 1L;
            return 0L;
        } catch (IOException e) {
            log.error("Exception occurred while  for removing -" + value + "Key" + time + "Partition " + partitionNo + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }

    @Override
    public List<String> get(long time, int partitionNum) throws SchedulerException {
        List<String> entries = new ArrayList<String>();
        String startRow = getRowKey(START_STRING, time, partitionNum);
        String stopRow = getRowKey(END_STRING, time, partitionNum);
        Scan scan = HbaseUtils.getScanner(startRow, stopRow, columnFamily);
        ResultScanner resultScanner = null;
        try {
            resultScanner = hTable.getScanner(scan);
            Result result = resultScanner.next();
            while (null != result) {
                String value = getValue(result);
                if (null != value)
                    entries.add(value);
                result = resultScanner.next();
            }
        } catch (IOException e) {
            log.error("Exception occurred While  for reading N Key" + time + "Partition " + partitionNum + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if (null != resultScanner)
                resultScanner.close();
        }
        return entries;
    }


    public List<String> getNextN(long time, int partitionNum, int n) throws SchedulerException {
        List<String> entries = new ArrayList<String>();
        String startRow = getRowKey(START_STRING, time, partitionNum);
        String stopRow = getRowKey(END_STRING, time, partitionNum);
        Scan scan = HbaseUtils.getScanner(startRow, stopRow, columnFamily);
        ResultScanner resultScanner = null;
        try {
            resultScanner = hTable.getScanner(scan);
            Result[] results = resultScanner.next(n);
            for (Result result : results) {
                String value = getValue(result);
                if (null != value)
                    entries.add(value);
            }
        } catch (IOException e) {
            log.error("Exception occurred While  for reading N Key" + time + "Partition " + partitionNum + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if (null != resultScanner)
                resultScanner.close();
        }
        return entries;
    }

    public void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException {
        List<Delete> deletes = new ArrayList<Delete>();
        for (String value : values) {
            deletes.add(new Delete(Bytes.toBytes(getRowKey(value, time, partitionNum))));
        }
        try {
            hTable.delete(deletes);
        } catch (IOException e) {
            log.error("Exception occurred While  for acking  Key" + time + "Partition " + partitionNum + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }


    private HTable getHTable() throws IOException {
        HTable table = (HTable) hConnection.getTable(TableName.valueOf(tableName));
        table.setAutoFlushTo(false);
        return table;
    }

    private void releaseHTableInterface(Table hTable) throws SchedulerException {
        try {
            if (null != hTable)
                hTable.close();
        } catch (IOException e) {
            log.error("Exception occurred While closing hTable interface - " + tableName + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }
    
    /*
     * Helper Functions
     */

    private String getRowKey(String value, long time, int partitionNo) {
        return schedulerInstance + DELIMITER + partitionNo + DELIMITER + Long.toString(time) + DELIMITER + value;
    }

    private String getValue(Result result) throws SchedulerException {
        String rowKey = new String(result.getRow());
        String[] token = rowKey.split(DELIMITER, 4);
        if (token.length == 4)
            return token[3];
        else {
            //For any invalid entry
            log.error("INVALID ENTRY : Exception occurred while reading row -" + rowKey);
            try {
                hTable.delete(new Delete(result.getRow()));
            } catch (IOException e) {
                log.error("Error in deleting invalid entry " + rowKey + " -" + e.fillInStackTrace());
                throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
            }
            return null;
        }
    }
}
