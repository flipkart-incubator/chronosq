package flipkart.cp.convert.chronosQ.impl.redis;

import flipkart.cp.convert.chronosQ.core.SchedulerCheckpointer;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;


public class RedisSchedulerCheckpoint implements SchedulerCheckpointer {

    private final String timerKeyPrefix;
    static Logger log = LoggerFactory.getLogger(RedisSchedulerCheckpoint.class.getName());
    private final Pool<Jedis> pool;

    public RedisSchedulerCheckpoint(Pool<Jedis> pool, String timerKeyPrefix) {
        this.pool = pool;
        this.timerKeyPrefix = timerKeyPrefix;
    }

    @Override
    public String peek(int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        String key = _key(timerKeyPrefix, partitionNum);
        try {
            jedis = pool.getResource();
            String value = jedis.get(key);
            log.info("Fetching value for key " + key + "is" + value);
            return value;
        } catch (Exception ex) {
            log.error("Exception occurred for " + key + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_CHECKPOINT_ERROR);
        } finally {
            if (jedis != null)
                jedis.close();
        }
    }

    private static String _key(String timerKeyPrefix, int partitionNum) {
        return timerKeyPrefix + "-" + partitionNum;
    }

    @Override
    public void set(String value, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        String key = _key(timerKeyPrefix, partitionNum);
        try {
            jedis = pool.getResource();
            jedis.set(key, value);
            log.info("Setting value to key " + key + " to-" + value);
        } catch (Exception ex) {
            log.error("Exception occurred for " + key + "-" + value + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_CHECKPOINT_ERROR);
        } finally {
            if (jedis != null)
                jedis.close();
        }
    }


}
