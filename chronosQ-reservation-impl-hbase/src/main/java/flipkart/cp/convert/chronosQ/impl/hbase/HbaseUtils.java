package flipkart.cp.convert.chronosQ.impl.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Map;

public  class HbaseUtils {
    public static Scan getScanner(String startRow, String stopRow ,byte[] columnFamily){
        Scan scanner = new Scan();
        scanner.addFamily(columnFamily);
        scanner.setStartRow(Bytes.toBytes(startRow));
        scanner.setStopRow(Bytes.toBytes(stopRow));
        scanner.setFilter(new FirstKeyOnlyFilter());
        return scanner;
    }

    public static Put createPut(String rowkey,byte[] columnFamily, Map<byte[], byte[]> data){
        Put cellput = new Put(Bytes.toBytes(rowkey));
        for(Map.Entry<byte[] , byte[]> entry : data.entrySet())  {
            cellput.add(columnFamily,entry.getKey(),entry.getValue());
        }
        return cellput;
    }

    public static Get createGet(String rowKey,byte[]columnFamily ){
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(columnFamily);
        return get;
    }
}
