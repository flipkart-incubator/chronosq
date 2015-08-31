package flipkart.cp.convert.chronosQ.impl.hbase;

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.List;

/**
 * Created by tushar.mandar on 2/26/15.
 */
public class HbaseSchedulerExample {
    public static void main(String args[]) throws SchedulerException {
        HTablePool htablePool = null;
        Configuration hconfig = HBaseConfiguration.create();
        hconfig.set("hbase.rootdir","hdfs://hadoop2.stage.ch.flipkart.com/hbase");
        hconfig.set("hbase.master.port","60000");
        hconfig.set("hbase.zookeeper.quorum","hadoop1.stage.ch.flipkart.com");
        hconfig.set("hbase.regionserver.lease.period","900000");
        hconfig.set("hbase.rpc.timeout","900000");
        hconfig.set("hadoop.security.authentication","kerberos");
        hconfig.set("hbase.security.authentication","kerberos");
        hconfig.set("hbase.master.kerberos.principal","hbase/_HOST@STAGE.CH.FLIPKART.COM");
        hconfig.set("hbase.regionserver.kerberos.principal","hbase/_HOST@STAGE.CH.FLIPKART.COM");
        hconfig.set("hbase.rpc.engine","org.apache.hadoop.hbase.ipc.SecureRpcEngine");
        System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
        UserGroupInformation.setConfiguration(hconfig);
        try {
            UserGroupInformation.loginUserFromKeytab(
                    "fk-w3-bro@STAGE.CH.FLIPKART.COM", "/var/lib/fk-w3-bro-commons/kerberos/stage/fk-w3-bro.keytab");
            htablePool =  new HTablePool(hconfig, 10);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String instanceName="instance1";
        Long time = System.currentTimeMillis();

        //Scheduler Store
        HbaseSchedulerStore hbaseSchedulerStore =  new HbaseSchedulerStore(htablePool,"w3_bro_EntityStateArchival","d",instanceName);
        hbaseSchedulerStore.add("entry1",time,1);
        hbaseSchedulerStore.remove("entry1", time, 1);

        hbaseSchedulerStore.add("entry2",time,2);
        hbaseSchedulerStore.add("entry3",time,2);
        hbaseSchedulerStore.add("entry4", time, 2);

        List<String> entries = hbaseSchedulerStore.get(time,2);
        for(String entry : entries){
            System.out.println("GET Response: " + entry);
        }

        entries =hbaseSchedulerStore.getNextN(time,2,2);
        for(String entry : entries){
            System.out.println("GET Next 2 Response: " + entry);
        }

        hbaseSchedulerStore.removeBulk(time , 2, entries);
        entries =hbaseSchedulerStore.getNextN(time,2,2);
        System.out.println("GET Next 2 Response size after deleting 2: " + entries.size() );
        hbaseSchedulerStore.removeBulk(time , 2, entries);

        entries =hbaseSchedulerStore.getNextN(time,2,2);
        System.out.println("GET Next 2 Response size after deleting all: " + entries.size() );
        hbaseSchedulerStore.removeBulk(time , 2, entries);


        //scheduler checkpointer
        HbaseSchedulerCheckpoint hbaseSchedulerCheckpoint =  new HbaseSchedulerCheckpoint(htablePool,"w3_bro_EntityStateArchival","d",instanceName);
        hbaseSchedulerCheckpoint.set(Long.toString(time),2);
        String value = hbaseSchedulerCheckpoint.peek(2);
        System.out.println("peek value: " + value);
        hbaseSchedulerCheckpoint.set(Long.toString(time + 100L),2);
    }
}
