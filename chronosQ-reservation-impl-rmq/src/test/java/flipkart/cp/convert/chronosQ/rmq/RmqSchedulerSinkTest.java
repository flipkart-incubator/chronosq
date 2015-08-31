package flipkart.cp.convert.chronosQ.rmq;

import com.rabbitmq.client.Channel;
import flipkart.cp.convert.chronosQ.core.SchedulerSink;
import flipkart.cp.convert.chronosQ.impl.rmq.RmqSchedulerSink;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 1:52 PM
 * To change this template use File | Settings | File Templates.
 */
public class RmqSchedulerSinkTest {
    @Mock
    private Channel channel;
    private SchedulerSink schedulerSink;
    private String exchangeName = "exchange";
    private String routingKeyName = "routingKey";
    private String value = "test";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        schedulerSink = new RmqSchedulerSink(channel, exchangeName, routingKeyName);
        doNothing().when(channel).basicPublish(exchangeName, routingKeyName, null, value.getBytes());
    }

    @Test
    public void testGiveForProcessing() throws Exception {
        checkChannelForProcessing();
    }

    @Test(expected = SchedulerException.class)
    public void testGiveForProcessingException() throws Exception {
        doThrow(new IOException("IO Error")).when(channel).basicPublish(exchangeName, routingKeyName, null, value.getBytes());
        checkChannelForProcessing();
    }

    @Test
    public void testGiveForProcessingList() throws Exception {
        List<String> values = new ArrayList<String>(1);
        values.add(value);
        doNothing().when(channel).basicPublish(exchangeName, routingKeyName, null, value.getBytes());
        schedulerSink.giveExpiredListForProcessing(values);
        verify(channel).basicPublish(exchangeName, routingKeyName, null, value.getBytes());
        verifyZeroInteractions(channel);
    }

    private void checkChannelForProcessing() throws Exception {
        schedulerSink.giveExpiredForProcessing(value);
        verify(channel).basicPublish(exchangeName, routingKeyName, null, value.getBytes());
        verifyZeroInteractions(channel);
    }
}
