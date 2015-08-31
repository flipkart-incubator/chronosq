package flipkart.cp.convert.chronosQ.impl.hbase;

import flipkart.cp.convert.chronosQ.core.SchedulerCheckpointer;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
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

    final HTablePool hTablePool;
    final String tableName;
    final byte[] columnFamily;
    final String instanceName;


    public HbaseSchedulerCheckpoint(HTablePool hTablePool, String tableName, String columnFamily, String instanceName){
        this.hTablePool = hTablePool;
        this.tableName = tableName;
        this.columnFamily = Bytes.toBytes(columnFamily);
        this.instanceName = instanceName;
    }

    @Override
    public String peek(int partitionNum) throws SchedulerException {
        String timerKey = getTimerKey(partitionNum);
        HTableInterface hTableInterface = getHTableInterface();
        try {
            Result result =  hTableInterface.get(HbaseUtils.createGet(timerKey,columnFamily));
            NavigableMap<byte[],byte[]> fmap = result.getFamilyMap(columnFamily);
            String value = new String(fmap.get(QUALIFIER));
            log.info("Fetching value for key " + timerKey + "is-" + value);
            return value;
        } catch (Exception ex) {
            log.error("Exception occurred for " + timerKey + ex.fillInStackTrace());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_CHECKPOINT_ERROR);
        }   finally {
            releaseHTableInterface(hTableInterface);
        }
    }

    @Override
    public void set( String value, int partitionNum) throws SchedulerException {
        String timerKey = getTimerKey(partitionNum);
        Map<byte[], byte[]>  data = new HashMap<byte[], byte[]> ();
        data.put(QUALIFIER, Bytes.toBytes(value));
        HTableInterface hTableInterface = getHTableInterface();
        try {
            hTableInterface.put(HbaseUtils.createPut(timerKey, columnFamily, data));
            log.info("Setting value to key " + timerKey + " to-" + value);
        } catch (Exception ex) {
            log.error("Exception occurred for " + timerKey + "-" + value + ex.fillInStackTrace());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_CHECKPOINT_ERROR);
        }  finally {
            releaseHTableInterface(hTableInterface);
        }

    }

    private HTableInterface getHTableInterface(){
        return  hTablePool.getTable(tableName);
    }

    private void  releaseHTableInterface(HTableInterface hTableInterface) throws SchedulerException {
        try {
            if(null != hTableInterface)
                hTableInterface.close();
        } catch (IOException e) {
            log.error("Exception occurred While closing htable interface - " +tableName + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_CHECKPOINT_ERROR);
        }
    }

    private String  getTimerKey(int partitionNum){
        return this.instanceName + DELIMITER + Integer.toString(partitionNum);
    }

}
