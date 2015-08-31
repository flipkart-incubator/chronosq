package flipkart.cp.convert.chronosQ.core.impl;


import flipkart.cp.convert.chronosQ.core.TimeBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecondGroupedTimeBucket implements TimeBucket {

    private final int numOfSecsForABucket;
    private static Logger log = LoggerFactory.getLogger(SecondGroupedTimeBucket.class.getName());

    public SecondGroupedTimeBucket(int numOfSecsForABucket) {
        this.numOfSecsForABucket = numOfSecsForABucket;
    }

    @Override
    public long toBucket(long epochTimestamp) {
        long result = epochTimestamp - (epochTimestamp % (numOfSecsForABucket * 1000));
        log.info("Value " + epochTimestamp + "is bucketed to " + result);
        return result;
    }

    @Override
    public long next(long epochTimestamp) {
        long result = toBucket(epochTimestamp) + (numOfSecsForABucket * 1000);
        log.info("Value " + epochTimestamp + "Next bucket is " + result);
        return result;
    }

}
