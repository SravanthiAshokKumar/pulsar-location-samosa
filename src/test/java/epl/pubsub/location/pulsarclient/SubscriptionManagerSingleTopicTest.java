package epl.pubsub.location;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import java.util.concurrent.atomic.AtomicBoolean;

import epl.pubsub.location.pulsarclient.SubscriptionChangedCallback;
import epl.pubsub.location.indexperf.Index;
import epl.pubsub.location.indexperf.IndexFactory;
/**
 * Unit test for simple App.
 */
public class SubscriptionManagerSingleTopicTest 
    extends TestCase implements SubscriptionChangedCallback<String>
{
    AtomicBoolean received = new AtomicBoolean();    
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public SubscriptionManagerSingleTopicTest( String testName )
    {
        super( testName );
        received.set(false);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( SubscriptionManagerSingleTopicTest.class );
    }

    /**
     * Rigourous Test :-)
     */
   
    @Override 
    public void onSubscriptionChange(String oldTopic, String newTopic){
        received.set(true);
    }

    public void testCreation()
    {

        Properties props = new Properties();
        Index index = IndexFactory.getInitializedIndex(33.7756, 84.3963, 34.7756, 85.3963, 0.01, IndexFactory.IndexType.GEOHASH, props);
     
        String locationFile = "data/output/0.0_0.txt";
        LocationSubscriptionHandlerSingleTopicImpl handler = new LocationSubscriptionHandlerSingleTopicImpl(index);
        handler.initSubscriptionChangedCallback(this);
        LocationManager lm = new LocationManagerImpl(1, locationFile);
        lm.initManager(handler);
        lm.start();
        lm.monitorLocation();
        
        assertTrue(received.get());
    }
}
