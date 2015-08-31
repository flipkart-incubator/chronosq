package flipkart.cp.convert.chronosQ.impl.hbase;

import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class HbaseSchedulerStore implements SchedulerStore {

    static Logger log = LoggerFactory.getLogger(HbaseSchedulerStore.class.getSimpleName());
    static private final String START_STRING ="0";
    static private final String END_STRING ="~";
    static private final String DELIMITER ="__";
    private final HTablePool hTablePool;
    private final String tableName;
    private final byte[] columnFamily;
    private final String schedulerInstance;
    private Map<byte[],byte[]> dummyData = new HashMap<byte[], byte[]>();



    public HbaseSchedulerStore(HTablePool hTablePool, String tableName, String columnFamily, String schedulerInstance ) {
        this.hTablePool = hTablePool;
        this.tableName = tableName;
        this.schedulerInstance = schedulerInstance;
        this.columnFamily = Bytes.toBytes(columnFamily);
        dummyData.put(Bytes.toBytes("d"),Bytes.toBytes(true));
    }


    @Override
    public void add(String value, long time, int partitionNo) throws SchedulerException {
        String rowKey = getRowKey(value,time,partitionNo);
        HTableInterface hTableInterface = getHTableInterface();
        try {
            hTableInterface.put(HbaseUtils.createPut(rowKey, columnFamily, dummyData));
        } catch (IOException e) {
            log.error("Exception occurred  for adding  -" + value + "Key" + time + "Partition " + partitionNo + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }  finally {
            releaseHTableInterface(hTableInterface);
        }
    }

    @Override
    public Long update(String value, long oldTime, long newTime, int partitionNo) throws SchedulerException {
        add(value,newTime ,partitionNo);
        return remove(value, oldTime, partitionNo);
    }

    @Override
    public Long remove(String value, long time, int partitionNo) throws SchedulerException {
        String rowKey =  getRowKey(value,time,partitionNo);
        HTableInterface hTableInterface = getHTableInterface();
        try {
            hTableInterface.delete(new Delete(Bytes.toBytes(rowKey)));
        } catch (IOException e) {
            log.error("Exception occurred While  for removing -" + value + "Key" + time + "Partition " + partitionNo + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            releaseHTableInterface(hTableInterface);
        }
        return 0L;
    }

    @Override
    public List<String> get(long time, int partitionNum) throws SchedulerException {
        List<String> entries = new ArrayList<String>();
        String startRow = getRowKey(START_STRING,time,partitionNum);
        String stopRow = getRowKey(END_STRING,time,partitionNum);
        Scan scan = HbaseUtils.getScanner(startRow, stopRow, columnFamily);
        ResultScanner resultScanner = null;
        HTableInterface hTableInterface = getHTableInterface();
        try {
            resultScanner = hTableInterface.getScanner(scan);
            Result result = resultScanner.next();
            while (null != result) {
                String value  = getValue(result);
                if(null != value )
                    entries.add(value);
                result = resultScanner.next();
            }
        } catch (IOException e) {
            log.error("Exception occurred While  for reading N Key" + time + "Partition " + partitionNum + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }finally {
            if( null !=  resultScanner)
                resultScanner.close();
            releaseHTableInterface(hTableInterface);
        }
        return entries;
    }


    public List<String> getNextN(long time, int partitionNum, int n) throws SchedulerException {
        List<String> entries = new ArrayList<String>();
        String startRow = getRowKey(START_STRING,time,partitionNum);
        String stopRow = getRowKey(END_STRING,time,partitionNum);
        Scan scan = HbaseUtils.getScanner(startRow, stopRow, columnFamily);
        HTableInterface hTableInterface = getHTableInterface();
        ResultScanner resultScanner = null;
        try {
            resultScanner = hTableInterface.getScanner(scan);
            Result[] results = resultScanner.next(n);
            for (Result result : results) {
                String value  = getValue(result);
                if(null != value )
                    entries.add(value);
            }
        } catch (IOException e) {
            log.error("Exception occurred While  for reading N Key" + time + "Partition " + partitionNum + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }finally {
            if(null !=  resultScanner)
                resultScanner.close();
            releaseHTableInterface(hTableInterface);
        }
        return entries;
    }

    public void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException{
        List<Delete> deletes = new ArrayList<Delete>();
        for(String value: values ){
            deletes.add(new Delete(Bytes.toBytes(getRowKey(value, time, partitionNum))));
        }
        HTableInterface hTableInterface = getHTableInterface();
        try {
            hTableInterface.delete(deletes);
        } catch (IOException e) {
            log.error("Exception occurred While  for acking  Key" + time + "Partition " + partitionNum + "-" + e.fillInStackTrace());
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
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
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }
    
    /*
     * Helper Functions
     */

    private String getRowKey(String value, long time, int partitionNo)  {
        return schedulerInstance + DELIMITER + partitionNo + DELIMITER + Long.toString(time) + DELIMITER + value;
    }

    private String getValue(Result result) throws SchedulerException{
        String rowKey = new String(result.getRow());
        String[] token = rowKey.split(DELIMITER,4);
        if(token.length == 4)
            return token[3];
        else {
            //For any invalid entry
            log.error("INVALID ENTRY : Exception occurred While for reading row -" + rowKey);
            HTableInterface hTableInterface = getHTableInterface();
            try {
                hTableInterface.delete(new Delete(result.getRow()));
            } catch (IOException e) {
                log.error("Error in deleting Invalid Entry " + rowKey + " -" + e.fillInStackTrace());
                throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
            } finally {
                releaseHTableInterface(hTableInterface);
            }
            return null;
        }
    }
}
