package flipkart.cp.convert.chronosQ.core.impl;

import flipkart.cp.convert.chronosQ.core.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomPartitioner implements Partitioner {

    private final int numOfPartitions;
    static Logger log = LoggerFactory.getLogger(RandomPartitioner.class.getName());

    public RandomPartitioner(int numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    @Override
    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    @Override
    public int getPartition(String entry) {
        int partition = ((int) System.nanoTime() % numOfPartitions);
        log.info("Partion for " + entry + "-" + partition);
        return partition;
    }
}
