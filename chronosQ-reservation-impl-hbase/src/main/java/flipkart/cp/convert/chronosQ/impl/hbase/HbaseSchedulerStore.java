package flipkart.cp.convert.chronosQ.impl.hbase;

import flipkart.cp.convert.chronosQ.core.DefaultSchedulerEntry;
import flipkart.cp.convert.chronosQ.core.SchedulerEntry;
import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


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


    public HbaseSchedulerStore(Connection hConnection, String tableName, String columnFamily, String schedulerInstance) {
        this.hConnection = hConnection;
        this.tableName = tableName;
        this.schedulerInstance = schedulerInstance;
        this.columnFamily = Bytes.toBytes(columnFamily);
        this.column = Bytes.toBytes("d");
        dummyData.put(column, Bytes.toBytes(true));
    }


    @Override
    public void add(SchedulerEntry value, long time, int partitionNo) throws SchedulerException {
        String rowKey = getRowKey(value.getKey(), time, partitionNo);
        Table hTable = null;
        try {
            hTable = getHTable();
            if (!value.getPayload().equals(value.getKey())) {
                hTable.put(HbaseUtils.createPut(rowKey, columnFamily, Collections.singletonMap(column, value.getPayload().getBytes())));
            } else {
                hTable.put(HbaseUtils.createPut(rowKey, columnFamily, dummyData));
            }
        } catch (IOException e) {
            log.error("Exception occurred  for adding  -" + value + "Key" + time + "Partition " + partitionNo + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            releaseHTableInterface(hTable);
        }
    }

    @Override
    public Long update(SchedulerEntry value, long oldTime, long newTime, int partitionNo) throws SchedulerException {
        add(value, newTime, partitionNo);
        return remove(value.getKey(), oldTime, partitionNo);
    }

    @Override
    public Long remove(String value, long time, int partitionNo) throws SchedulerException {
        String rowKey = getRowKey(value, time, partitionNo);
        Table hTable = null;
        try {
            hTable = getHTable();
            hTable.delete(new Delete(Bytes.toBytes(rowKey)));
            return 1L;
        } catch (IOException e) {
            log.error("Exception occurred while  for removing -" + value + "Key" + time + "Partition " + partitionNo + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            releaseHTableInterface(hTable);
        }
    }

    @Override
    public List<SchedulerEntry> get(long time, int partitionNum) throws SchedulerException {
        List<SchedulerEntry> entries = new ArrayList<>();
        String startRow = getRowKey(START_STRING, time, partitionNum);
        String stopRow = getRowKey(END_STRING, time, partitionNum);
        Scan scan = HbaseUtils.getScanner(startRow, stopRow, columnFamily);
        ResultScanner resultScanner = null;
        Table hTable = null;
        try {
            hTable = getHTable();
            resultScanner = hTable.getScanner(scan);
            Result result = resultScanner.next();
            while (null != result) {
                SchedulerEntry value = getValue(result);
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
            releaseHTableInterface(hTable);
        }
        return entries;
    }


    public List<SchedulerEntry> getNextN(long time, int partitionNum, int n) throws SchedulerException {
        List<SchedulerEntry> entries = new ArrayList<>();
        String startRow = getRowKey(START_STRING, time, partitionNum);
        String stopRow = getRowKey(END_STRING, time, partitionNum);
        Scan scan = HbaseUtils.getScanner(startRow, stopRow, columnFamily);
        Table hTable = null;
        ResultScanner resultScanner = null;
        try {
            hTable = getHTable();
            resultScanner = hTable.getScanner(scan);
            Result[] results = resultScanner.next(n);
            for (Result result : results) {
                SchedulerEntry value = getValue(result);
                if (null != value)
                    entries.add(value);
            }
        } catch (IOException e) {
            log.error("Exception occurred While  for reading N Key" + time + "Partition " + partitionNum + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if (null != resultScanner)
                resultScanner.close();
            releaseHTableInterface(hTable);
        }
        return entries;
    }

    public void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException {
        List<Delete> deletes = new ArrayList<Delete>();
        for (String value : values) {
            deletes.add(new Delete(Bytes.toBytes(getRowKey(value, time, partitionNum))));
        }
        Table hTable = null;
        try {
            hTable = getHTable();
            hTable.delete(deletes);
        } catch (IOException e) {
            log.error("Exception occurred While  for acking  Key" + time + "Partition " + partitionNum + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            releaseHTableInterface(hTable);
        }
    }


    private Table getHTable() throws IOException {
        return hConnection.getTable(TableName.valueOf(tableName));
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

    private String getRowKey(String key, long time, int partitionNo) {
        return schedulerInstance + DELIMITER + partitionNo + DELIMITER + Long.toString(time) + DELIMITER + key;
    }

    private SchedulerEntry getValue(Result result) throws SchedulerException {
        String rowKey = new String(result.getRow());
        String[] token = rowKey.split(DELIMITER, 4);
        if (token.length == 4) {
            byte[] payload = result.getValue(columnFamily, column);
            if (Arrays.equals(Bytes.toBytes(true), payload))
                return new DefaultSchedulerEntry(token[3]);
            else
                return new DefaultSchedulerEntry(token[3], new String(payload));
        } else {
            //For any invalid entry
            log.error("INVALID ENTRY : Exception occurred while reading row -" + rowKey);
            Table hTable = null;
            try {
                hTable = getHTable();
                hTable.delete(new Delete(result.getRow()));
            } catch (IOException e) {
                log.error("Error in deleting invalid entry " + rowKey + " -" + e.fillInStackTrace());
                throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
            } finally {
                releaseHTableInterface(hTable);
            }
            return null;
        }
    }
}
