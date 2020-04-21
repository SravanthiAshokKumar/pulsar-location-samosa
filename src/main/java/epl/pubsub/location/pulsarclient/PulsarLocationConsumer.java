package epl.pubsub.location.pulsarclient;

import java.util.concurrent.CompletableFuture;

import java.util.List;

public interface PulsarLocationConsumer extends SubscriptionChangedCallback<List<String>>{

    public void start(List<String> topic, String subscriptionName, MessageCallback onMessageReceived);
    public void shutdown();

}
