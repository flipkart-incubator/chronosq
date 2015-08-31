package flipkart.cp.convert.chronosQ.impl.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.util.Pool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 24/02/15
 * Time: 7:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class RedisParitioner {

    private final List<Pool<Jedis>> poolList;
    private final int numOfConnections;

    public void addInstance(JedisShardInfo config) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(numOfConnections);
        JedisPool jedisPool = new JedisPool(poolConfig, config.getHost(), config.getPort());
        poolList.add(jedisPool);
    }

    public void addInstance(Pool<Jedis> pool) {
        poolList.add(pool);
    }

    public RedisParitioner(int numOfConnections) {
        this.numOfConnections = numOfConnections;
        poolList = new ArrayList<Pool<Jedis>>();
    }

    public RedisParitioner(List<Pool<Jedis>> poolList, int numOfConnections) {
        this.poolList = poolList;
        this.numOfConnections = numOfConnections;
    }

    public RedisParitioner(List<Pool<Jedis>> poolList) {
        this.poolList = poolList;
        this.numOfConnections = 0;
    }

    public Pool<Jedis> getJedis(int partitionNum) {
        int hashed = partitionNum % poolList.size();
        return poolList.get(hashed);
    }

}
