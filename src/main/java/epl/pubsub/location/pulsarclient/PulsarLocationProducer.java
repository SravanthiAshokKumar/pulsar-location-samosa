package epl.pubsub.location.pulsarclient;

import java.util.concurrent.CompletableFuture;

import java.util.List;

public interface PulsarLocationProducer extends SubscriptionChangedCallback<String>{

    public void start(String topic);
    
    public void shutdown();

    public void sendMessage(byte[] payload);
    
    public void disableMetricCollection();

    public ProducerMetrics getProducerMetrics();
}
