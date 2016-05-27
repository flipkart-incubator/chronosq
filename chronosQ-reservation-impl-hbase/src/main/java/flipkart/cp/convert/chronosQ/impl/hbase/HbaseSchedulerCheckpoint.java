package flipkart.cp.convert.chronosQ.impl.hbase;

import flipkart.cp.convert.chronosQ.core.SchedulerCheckpointer;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;


public class HbaseSchedulerCheckpoint implements SchedulerCheckpointer {

    static Logger log = LoggerFactory.getLogger(HbaseSchedulerCheckpoint.class.getSimpleName());
    static final String DELIMITER="__";
    static final byte[] QUALIFIER = Bytes.toBytes("val");

    final Connection hConnection;
    final String tableName;
    final byte[] columnFamily;
    final String instanceName;


    public HbaseSchedulerCheckpoint(Connection hConnection, String tableName, String columnFamily, String instanceName){
        this.hConnection = hConnection;
        this.tableName = tableName;
        this.columnFamily = Bytes.toBytes(columnFamily);
        this.instanceName = instanceName;
    }

    @Override
    public String peek(int partitionNum) throws SchedulerException {
        String timerKey = getTimerKey(partitionNum);
        Table hTable = null;
        try {
            hTable = getHTable();
            Result result =  hTable.get(HbaseUtils.createGet(timerKey, columnFamily));
            NavigableMap<byte[],byte[]> fMap = result.getFamilyMap(columnFamily);
            String value = new String(fMap.get(QUALIFIER));
            log.info("Fetching value for key " + timerKey + "is-" + value);
            return value;
        } catch (Exception ex) {
            log.error("Exception occurred for " + timerKey + ex.fillInStackTrace());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_CHECKPOINT_ERROR);
        }   finally {
            releaseHTableInterface(hTable);
        }
    }

    @Override
    public void set(String value, int partitionNum) throws SchedulerException {
        String timerKey = getTimerKey(partitionNum);
        Map<byte[], byte[]>  data = new HashMap<>();
        data.put(QUALIFIER, Bytes.toBytes(value));
        Table hTable = null;
        try {
            hTable = getHTable();
            hTable.put(HbaseUtils.createPut(timerKey, columnFamily, data));
            log.info("Setting value to key " + timerKey + " to-" + value);
        } catch (Exception ex) {
            log.error("Exception occurred for " + timerKey + "-" + value + ex.fillInStackTrace());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_CHECKPOINT_ERROR);
        }  finally {
            releaseHTableInterface(hTable);
        }

    }

    private Table getHTable() throws IOException {
        return  hConnection.getTable(TableName.valueOf(tableName));
    }

    private void  releaseHTableInterface(Table hTable) throws SchedulerException {
        try {
            if(null != hTable)
                hTable.close();
        } catch (IOException e) {
            log.error("Exception occurred While closing hTable - " +tableName + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_CHECKPOINT_ERROR);
        }
    }

    private String  getTimerKey(int partitionNum){
        return this.instanceName + DELIMITER + Integer.toString(partitionNum);
    }

}
