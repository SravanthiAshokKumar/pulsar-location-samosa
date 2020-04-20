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

import epl.pubsub.location.indexperf.Index;
import epl.pubsub.location.indexperf.IndexFactory;
/**
 * Unit test for simple App.
 */
public class LocationManagerTest 
    extends TestCase implements LocationChangedCallback
{
    AtomicBoolean received = new AtomicBoolean();    
    String testPayload = "testpayload";
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public LocationManagerTest( String testName )
    {
        super( testName );
        received.set(false);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( LocationManagerTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    
    public void onLocationChange(Location oldLoc, Location newLoc){
        received.set(true);
    }
    public void testCreation()
    {

            Properties props = new Properties();
            //Index index = IndexFactory.getInitializedIndex(33.7756, 84.3963, 34.7756, 85.3963, 0.01, IndexFactory.IndexType.GEOHASH, props);
     
            String locationFile = "data/output/0.0_0.txt";
            LocationManager lm = new LocationManagerImpl(1, locationFile);
            lm.initManager(this);
            lm.start();
            lm.monitorLocation();
            
            assertTrue(received.get());
    }
}
