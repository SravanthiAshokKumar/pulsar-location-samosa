package epl.pubsub.location.pulsarclient;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;


import org.apache.pulsar.client.api.Message;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit test for simple App.
 */
public class LocationConsumerTest 
    extends TestCase implements MessageCallback
{
    AtomicBoolean received = new AtomicBoolean();    
    String testPayload = "testpayload";
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public LocationConsumerTest( String testName )
    {
        super( testName );
        received.set(false);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( LocationConsumerTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    
    public void onMessageReceived(Message<byte[]> payload){
        received.set(true);
        //assert(Arrays.toString(payload.getData()).equals(this.testPayload));
    }
    public void testCreation()
    {
        try{
            System.out.println("in test");
            String configFile = "pulsar.yaml";
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            PulsarConfig pulsarConfig = mapper.readValue(new File(configFile), PulsarConfig.class);
            PulsarLocationClient client = new PulsarLocationClientImpl();
            client.initClient(pulsarConfig);
        
            PulsarLocationProducer producer = client.getNewProducer();
            producer.start("testtopic");
            producer.sendMessage(testPayload.getBytes());
//            producer.shutdown(); 
            PulsarLocationConsumer consumer  = client.getNewConsumer();
            List<String> topics = new ArrayList<>();
            topics.add("testtopic");
            consumer.start(topics, "testSub", this);
            while(received.get() == false){
                Thread.sleep(10);
            }
            consumer.shutdown();
            assertTrue( true );
           
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        } catch(InterruptedException ex){
            System.out.println(ex.getMessage());
        }
    }
}
