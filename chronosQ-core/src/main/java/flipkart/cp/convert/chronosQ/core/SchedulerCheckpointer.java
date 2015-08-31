package flipkart.cp.convert.chronosQ.core;


import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;

public interface SchedulerCheckpointer {

    String peek(int partitionNum) throws SchedulerException;

    void set(String value,int partitionNum) throws SchedulerException;

}
