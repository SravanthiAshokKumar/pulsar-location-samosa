package epl.pubsub.location.pulsarclient;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import epl.pubsub.location.indexperf.Index;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CompletableFuture;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.time.StopWatch;

import java.util.function.LongBinaryOperator;
import java.util.concurrent.TimeUnit;

class PulsarLocationConsumerImpl implements PulsarLocationConsumer {
    private static final Logger log = LoggerFactory.getLogger(PulsarLocationConsumerImpl.class);

    private Consumer<byte[]> currentConsumer;
    private ConsumerBuilder<byte[]> currentConsumerBuilder;

    private Consumer<byte[]> newConsumer;
    private ConsumerBuilder<byte[]> newConsumerBuilder;

    private PulsarClient client;

    private List<String> currentTopics;
    private List<String> newTopics;

    private ReentrantLock lock = new ReentrantLock();

    private ExecutorService executor;

    private MessageCallback callback;
    private String subscriptionName;

    private ConsumerMetrics consumerMetrics = new ConsumerMetrics();
    private LongBinaryOperator latencyAccumulator;
    private LongBinaryOperator maxValTester;

    private boolean disableMetrics = false;


    public PulsarLocationConsumerImpl(PulsarClient client){
        this.client = client;
        latencyAccumulator = (x,y) -> x + y;
        maxValTester = (x,y) -> x > y ? x : y;
    }

    @Override
    public void disableMetricCollection(){
        disableMetrics = true;
        log.info("disabled metrics");
    }

    private ConsumerBuilder createConsumerBuilder(List<String> topics, String subscriptionName, MessageCallback cb) {
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer().subscriptionType(SubscriptionType.Failover).messageListener((consumer, msg) ->{
            if(!disableMetrics){
                consumerMetrics.numMessagesConsumed.getAndIncrement();
                long latency = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - msg.getPublishTime()) / 1000;
                consumerMetrics.aggregateEndToEndLatency.getAndAccumulate(latency, latencyAccumulator);
                consumerMetrics.maxEndToEndLatency.getAndAccumulate(latency, maxValTester);
            }
            consumer.acknowledgeAsync(msg);
            cb.onMessageReceived(msg);
        }).topics(topics).subscriptionName(subscriptionName);

        return consumerBuilder;
    }
    
    @Override
    public void start(List<String> topics, String subscriptionName, MessageCallback cb){
        currentTopics = topics;
        newTopics = topics;
        callback = cb;
        this.subscriptionName = subscriptionName;
        log.info("sub name = {}", this.subscriptionName);
        currentConsumerBuilder = createConsumerBuilder(topics, subscriptionName, cb);
        try {
            currentConsumer = currentConsumerBuilder.subscribeAsync().get();
            newConsumerBuilder = currentConsumerBuilder;
            newConsumer = currentConsumer;
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public void shutdown(){
        try {
            currentConsumer.close();
        } catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public void onSubscriptionChange(List<String> oldTopics, List<String> newTopics){
        log.info("received topic switch event");
        StopWatch sw = new StopWatch();
        sw.start();
        switchTopic(newTopics);
        sw.stop();
        if(!disableMetrics){
            consumerMetrics.numTopicChanges.getAndIncrement();
            consumerMetrics.aggregateTopicChangeLatency.getAndAccumulate(sw.getTime(), latencyAccumulator);
            consumerMetrics.maxTopicChangeLatency.getAndAccumulate(sw.getTime(), maxValTester);
        }
        TopicSwitchTask task = new TopicSwitchTask();
        executor.execute(task);
    }
    private class TopicSwitchTask implements Runnable {
        @Override
        public void run() {
            reclaimConsumer(); 
        }
    } 

    private void switchTopic(List<String> newTopics){
        try{
            lock.lock();
            newConsumerBuilder =  createConsumerBuilder(newTopics, subscriptionName, callback);
            newConsumer = newConsumerBuilder.subscribeAsync().get();
            log.info("subbed");
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        finally{
            lock.unlock();
        }
    }
    
    private void reclaimConsumer() {
        try {
            lock.lock();
            currentConsumer.closeAsync();
            currentConsumerBuilder = newConsumerBuilder;
            currentConsumer = newConsumer;
        }catch(Exception ex){
            ex.printStackTrace();
        } finally{
            lock.unlock();
        }
        
    }
    
    @Override
    public ConsumerMetrics getConsumerMetrics(){
        return consumerMetrics;
    }
}
