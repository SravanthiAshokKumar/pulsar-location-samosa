package epl.pubsub.location.pulsarclient;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
/**
 * Unit test for simple App.
 */
public class LocationClientTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public LocationClientTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( LocationClientTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    
    public void testCreation()
    {
        try{
            String configFile = "pulsar.yaml";
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            PulsarConfig pulsarConfig = mapper.readValue(new File(configFile), PulsarConfig.class);
            PulsarLocationClient client = new PulsarLocationClientImpl();
            client.initClient(pulsarConfig);
            assertTrue( true );
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }
}
