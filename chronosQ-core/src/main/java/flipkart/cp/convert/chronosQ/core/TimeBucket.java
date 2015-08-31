package flipkart.cp.convert.chronosQ.core;

public interface TimeBucket {

    public long toBucket(long epochTimestamp);

    long next(long epochTimestamp);

}
