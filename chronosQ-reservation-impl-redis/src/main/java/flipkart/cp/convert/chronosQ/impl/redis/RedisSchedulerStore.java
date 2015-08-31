package flipkart.cp.convert.chronosQ.impl.redis;

import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.util.Pool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class RedisSchedulerStore implements SchedulerStore {
    private final RedisParitioner redisParitioner;
    static Logger log = LoggerFactory.getLogger(RedisSchedulerStore.class.getName());

    public RedisSchedulerStore(RedisParitioner redisParitioner) {
        this.redisParitioner = redisParitioner;
    }


    private void _releaseToPool(Jedis jedis, int partitionNum) throws SchedulerException {
        try {
            Pool<Jedis> jedisPool = redisParitioner.getJedis(partitionNum);
            jedisPool.returnResource(jedis);
        } catch (Exception e) {
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }

    private Jedis _getInstance(int partitionNum) throws SchedulerException {
        try {
            Pool<Jedis> jedisPool = redisParitioner.getJedis(partitionNum);
            return jedisPool.getResource();
        } catch (Exception e) {
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }


    @Override
    public void add(String value, long time, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        try {
            jedis = _getInstance(partitionNum);
            jedis.sadd(convertLongToString(time), value);
            log.info("Added value " + value + "To " + time);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + value + "Key" + time + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                _releaseToPool(jedis, partitionNum);
        }
    }

    @Override
    public Long update(String value, long oldTime, long newTime, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        Long result;
        try {
            jedis = _getInstance(partitionNum);
            result = jedis.smove(convertLongToString(oldTime), convertLongToString(newTime), value);
            log.info("Updated value " + value + "From " + oldTime + "To " + newTime);
            return result;
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + value + "Key" + oldTime + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                _releaseToPool(jedis, partitionNum);
        }

    }

    @Override
    public Long remove(String value, long time, int partitionNum) throws SchedulerException {
        Jedis jedis = null;

        try {
            jedis = _getInstance(partitionNum);
            Long result = jedis.srem(convertLongToString(time), value);
            log.info("Removed value " + value + "From" + time);
            return result;
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + value + "Key" + time + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                _releaseToPool(jedis, partitionNum);
        }
    }

    @Override
    public List<String> get(long time, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        Set<String> resultSet;
        try {
            jedis = _getInstance(partitionNum);
            resultSet = jedis.smembers(convertLongToString(time));
            log.info("Get For " + time + "-" + resultSet);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "Key" + time + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                _releaseToPool(jedis, partitionNum);
        }
        return new ArrayList<String>(resultSet);
    }

    @Override
    public List<String> getNextN(long time, int partitionNum, int n) throws SchedulerException {
        Jedis jedis = null;
        List<String> resultSet;
        try {
            jedis = _getInstance(partitionNum);
            resultSet = jedis.srandmember(convertLongToString(time), n);
            log.info("Get For " + time + "-" + resultSet);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "Key" + time + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                _releaseToPool(jedis, partitionNum);
        }
        return new ArrayList<String>(resultSet);
    }

    @Override
    public void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException {
        Jedis jedis = null;
        try {
            jedis = _getInstance(partitionNum);
            Pipeline pipeline = jedis.pipelined();
            for (String value : values)
                pipeline.srem(convertLongToString(time), value);
            log.info("Removed values " + values + "From" + time);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + values + "Key" + time + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                _releaseToPool(jedis, partitionNum);
        }
    }

    private static String convertLongToString(long time) {
        return String.valueOf(time);
    }

}
