package epl.pubsub.location.pulsarclient;

public class PulsarProducerConfig {
    public boolean batchingEnabled;
    public int maxBatchPublishDelay;
    public boolean blockIfQueueFull;
    public int maxPendingMessages; 
}
