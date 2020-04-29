package flipkart.cp.convert.chronosQ.redis;

import flipkart.cp.convert.chronosQ.core.SchedulerCheckpointer;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import flipkart.cp.convert.chronosQ.impl.redis.RedisSchedulerCheckpoint;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 12:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class RedisSchedulerCheckPointTest {
    @Mock
    private JedisPool jedisPool;
    private SchedulerCheckpointer schedulerCheckpointer;
    private Jedis jedis;
    private String value = "testvalue";
    private final int partitionNum = 1;

    @Before
    public void setUp()
            throws Exception {
        MockitoAnnotations.initMocks(this);
        schedulerCheckpointer = new RedisSchedulerCheckpoint(jedisPool, "TIMER_KEY_TEST");
        jedis = new Jedis("localhost", 6379);
        when(jedisPool.getResource()).thenReturn(jedis);
    }

    @Test
    public void setValueForKey() throws SchedulerException {
        schedulerCheckpointer.set(value, partitionNum);
        checkJedisFlow();
    }

    @Test
    public void getValueForKey() throws SchedulerException {
        String valuePeeked = schedulerCheckpointer.peek(partitionNum);
        assertEquals(valuePeeked, value);
        checkJedisFlow();
    }

    @Test(expected = SchedulerException.class)
    public void checkExceptionForPeek() throws SchedulerException {
        when(jedisPool.getResource()).thenReturn(null);
        schedulerCheckpointer.peek(partitionNum);
        checkJedisFlow();
    }

    @Test(expected = SchedulerException.class)
    public void checkExceptionForSet() throws SchedulerException {
        when(jedisPool.getResource()).thenReturn(null);
        schedulerCheckpointer.set(value, partitionNum);
        checkJedisFlow();
    }

    private void checkJedisFlow() {
        verify(jedisPool).getResource();
        //verify(jedisPool).returnResource(jedis);
        verifyZeroInteractions(jedisPool);
    }

}