package epl.pubsub.location.pulsarclient;

import java.util.concurrent.CompletableFuture;

import java.util.List;

public interface PulsarLocationProducer {

    public void start(String topic);
    
    public void shutdown();

    public CompletableFuture<Void> sendMessage(byte[] payload);

}
