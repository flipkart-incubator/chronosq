package flipkart.cp.convert.chronosQ.core.impl;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.core.Partitioner;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MurmurHashPartioner implements Partitioner {

    private final int numOfPartitions;
    static Logger log = LoggerFactory.getLogger(MurmurHashPartioner.class.getName());

    public MurmurHashPartioner(int numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    @Override
    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    @Override
    public int getPartition(String entry) throws SchedulerException{
        try {

        HashFunction hashFunction = Hashing.murmur3_128();
        HashCode hashCode = hashFunction.hashBytes(entry.getBytes());
        int partition = Math.abs(hashCode.asInt()) % numOfPartitions;
        log.debug("Partition for " + entry + ": " + partition);
        return partition;
        }
        catch (Exception ex)
        {
           log.error("Exception occurred " + ex.fillInStackTrace());
           throw new SchedulerException(ex, ErrorCode.PARTITIONER_ERROR);
        }
    }
}
