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

class PulsarLocationConsumerImpl implements PulsarLocationConsumer {
    private static final Logger log = LoggerFactory.getLogger(PulsarLocationConsumerImpl.class);

    private Consumer<byte[]> currentConsumer;
    private ConsumerBuilder<byte[]> currentConsumerBuilder;

    private Consumer<byte[]> newConsumer;
    private ConsumerBuilder<byte[]> newConsumerBuilder;

    private PulsarClient client;

    private List<String> currentTopics;
    private List<String> newTopics;

    ReentrantLock lock = new ReentrantLock();

    ExecutorService executor;

    MessageCallback callback;
    String subscriptionName;

    public PulsarLocationConsumerImpl(PulsarClient client){
        this.client = client;
    }
    private ConsumerBuilder createConsumerBuilder(List<String> topics, String subscriptionName, MessageCallback cb) {
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer().subscriptionType(SubscriptionType.Failover).messageListener((consumer, msg) ->{
            cb.onMessageReceived(msg);
            consumer.acknowledgeAsync(msg);
        }).topics(topics).subscriptionName(subscriptionName);

        return consumerBuilder;
    }
    
    @Override
    public void start(List<String> topics, String subscriptionName, MessageCallback cb){
        currentTopics = topics;
        newTopics = topics;
        callback = cb;
        subscriptionName = subscriptionName;
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
        switchTopic(newTopics);
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
        }catch(Exception ex){
            System.out.println(ex.getMessage());
        }
        finally{
            lock.unlock();
        }
    }
    
    private void reclaimConsumer() {
        try {
            currentConsumer.close();
            currentConsumerBuilder = newConsumerBuilder;
            currentConsumer = newConsumer;
        }catch(PulsarClientException ex){
            ex.printStackTrace();
        }
        
    }
}
