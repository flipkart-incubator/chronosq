package flipkart.cp.convert.chronosQ.core;

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;

public interface Partitioner {

    int getNumOfPartitions();

    int getPartition(String entry) throws SchedulerException;

}
