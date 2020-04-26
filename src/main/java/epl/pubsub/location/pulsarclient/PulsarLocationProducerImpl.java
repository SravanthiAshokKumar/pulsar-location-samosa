package epl.pubsub.location.pulsarclient;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.apache.commons.lang3.time.StopWatch;

import epl.pubsub.location.indexperf.Index;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongBinaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PulsarLocationProducerImpl implements  PulsarLocationProducer{
    private static final Logger log = LoggerFactory.getLogger(PulsarLocationProducerImpl.class);
    
    private Producer<byte[]> currentProducer;
    private ProducerBuilder<byte[]> currentProducerBuilder;

    private Producer<byte[]> newProducer;
    private ProducerBuilder<byte[]> newProducerBuilder;

    private PulsarProducerConfig config;

    private PulsarClient client;

    private String currentTopic;
    private String newTopic;
    private String topicPrefix;

    private ProducerMetrics producerMetrics = new ProducerMetrics();
    private boolean  disableMetrics = false;

    private ReentrantLock lock = new ReentrantLock();
    private AtomicBoolean isTransitioning = new AtomicBoolean();
    private LongBinaryOperator latencyAccumulator;
    private LongBinaryOperator maxValTester;

    ExecutorService executor;

    public PulsarLocationProducerImpl(PulsarClient client, PulsarProducerConfig config, String topicPrefix){
        this.client = client;
        this.config = config;
        this.topicPrefix = topicPrefix;
        isTransitioning.set(false);
        executor = Executors.newSingleThreadExecutor();
        latencyAccumulator =  (x, y) -> x + y; 
        maxValTester = (x, y) -> x > y ? x : y; 
    }

    @Override 
    public void disableMetricCollection() {
        disableMetrics = true;
        log.info("disabled metrics");
    }

    private ProducerBuilder<byte[]> createProducerBuilder() {
        ProducerBuilder<byte[]> producerBuilder = client.newProducer()
                               .enableBatching(config.batchingEnabled)
                               .batchingMaxPublishDelay(config.maxBatchPublishDelay, TimeUnit.MILLISECONDS)
                               .blockIfQueueFull(config.blockIfQueueFull)
                               .maxPendingMessages(config.maxPendingMessages);
        return producerBuilder;
            
    }
    @Override
    public void start(String topic){
        currentProducerBuilder = createProducerBuilder();
        currentTopic = topicPrefix + "/" + topic;
        newTopic = currentTopic;
        try {
            currentProducer = currentProducerBuilder.topic(topic).create();
            newProducerBuilder = currentProducerBuilder;
            newProducer = currentProducer;
        }catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public void shutdown(){
        try {
            currentProducer.flush();
            currentProducer.close();
            executor.shutdown();
        } catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public void onSubscriptionChange(String oldTopic, String newTopic){
        log.info("received subscription change event frm {} to {}", oldTopic, newTopic);
        TopicSwitchTask task = new TopicSwitchTask();
        StopWatch sw = new StopWatch();
        sw.start();
        switchTopic(newTopic);
        sw.stop();
        if(!disableMetrics){
            producerMetrics.aggregateTopicChangeLatency.getAndAccumulate(sw.getTime(), latencyAccumulator);
            producerMetrics.numTopicChanges.getAndIncrement();   
            producerMetrics.maxTopicChangeLatency.getAndAccumulate(sw.getTime(), maxValTester);
        }
        executor.execute(task);
    }
    private class TopicSwitchTask implements Runnable {
        @Override
        public void run() {
            reclaimProducer(); 
        }
    } 
    private void switchTopic(String topic){
        isTransitioning.set(true);
        try{
            lock.lock();
            currentTopic = topicPrefix + "/" + topic;
            newTopic = currentTopic;
            newProducerBuilder = createProducerBuilder();
            newProducer = newProducerBuilder.topic(newTopic).create();
        }catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
        finally{
            lock.unlock();
        }
    }
    
    private void reclaimProducer(){
        try{
            currentProducer.flush();
            currentProducer.close();
            currentProducerBuilder = newProducerBuilder;
            currentProducer = newProducer;
            isTransitioning.set(false);
        } catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
        finally{
    
        }
    }
    
    @Override
    public void sendMessage(byte[] payload) {
        StopWatch sw = new StopWatch();
        sw.start();
        if(isTransitioning.get() == false){
            currentProducer.newMessage().value(payload).sendAsync().thenApply(msgId->null);
        }
        else {
            try{
                lock.lock();
                newProducer.newMessage().value(payload).sendAsync().thenApply(msgId->null);
            } finally{
                lock.unlock();
            }
        }
        sw.stop();
        if(!disableMetrics){ 
            producerMetrics.maxPublishLatency.getAndAccumulate(sw.getTime(), maxValTester);
            producerMetrics.numMessagesPublished.getAndIncrement();
            producerMetrics.aggregatePublishLatency.getAndAccumulate(sw.getTime(), latencyAccumulator);
        }
    }

    @Override
    public ProducerMetrics getProducerMetrics(){
        return producerMetrics;
    }    
}
