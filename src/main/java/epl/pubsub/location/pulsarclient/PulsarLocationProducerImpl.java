package epl.pubsub.location.pulsarclient;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;


import epl.pubsub.location.indexperf.Index;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class PulsarLocationProducerImpl implements SubscriptionChangedCallback<String>, PulsarLocationProducer{


    private Producer<byte[]> currentProducer;
    private ProducerBuilder<byte[]> currentProducerBuilder;

    private Producer<byte[]> newProducer;
    private ProducerBuilder<byte[]> newProducerBuilder;

    private PulsarProducerConfig config;

    private PulsarClient client;

    private String currentTopic;
    private String newTopic;

    ReentrantLock lock = new ReentrantLock();
    AtomicBoolean isTransitioning = new AtomicBoolean();

    ExecutorService executor;

    public PulsarLocationProducerImpl(PulsarClient client, PulsarProducerConfig config){
        this.client = client;
        this.config = config;
        isTransitioning.set(false);
        executor = Executors.newSingleThreadExecutor();
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
        currentTopic = topic;
        newTopic = topic;
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
        } catch(PulsarClientException ex){
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public void onSubscriptionChange(String oldTopic, String newTopic){
        TopicSwitchTask task = new TopicSwitchTask();
        switchTopic(newTopic);
        executor.execute(task);
    }
    private class TopicSwitchTask implements Runnable {
        @Override
        public void run() {
            reclaimProducer(); 
        }
    } 
    private void switchTopic(String newTopic){
        isTransitioning.set(true);
        try{
            lock.lock();
            currentTopic = newTopic;
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
    public CompletableFuture<Void> sendMessage(byte[] payload) {
        if(isTransitioning.get() == false){
            return currentProducer.newMessage().value(payload).sendAsync().thenApply(msgId->null);
        }
        else {
            CompletableFuture<Void> future;
            try{
                lock.lock();
                future = newProducer.newMessage().value(payload).sendAsync().thenApply(msgId->null);
            } finally{
                lock.unlock();
            }
            return future;
        }
    }
    
}
