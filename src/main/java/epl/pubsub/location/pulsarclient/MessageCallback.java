package epl.pubsub.location.pulsarclient;

import org.apache.pulsar.client.api.Message;

public interface MessageCallback {
    void onMessageReceived(Message<byte[]> payload);
}
