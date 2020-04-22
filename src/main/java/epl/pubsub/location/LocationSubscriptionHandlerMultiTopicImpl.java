package epl.pubsub.location;

import epl.pubsub.location.indexperf.Index;
import java.util.List;

import epl.pubsub.location.pulsarclient.SubscriptionChangedCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationSubscriptionHandlerMultiTopicImpl implements  LocationSubscriptionHandler<List<String>> {
    private static final Logger log = LoggerFactory.getLogger(LocationSubscriptionHandlerMultiTopicImpl.class);

    private Index index;
    private SubscriptionChangedCallback<List<String>> callback;

    public LocationSubscriptionHandlerMultiTopicImpl(Index index){
        this.index = index;

    }
    public void initSubscriptionChangedCallback( SubscriptionChangedCallback<List<String>> callback) {
        this.callback = callback;
    }
    
    @Override
    public void onLocationChange(Location oldLocation, Location newLocation){
        log.info("multi topic location change");
        List<String> oldTopics = index.getNearestNeighbors(oldLocation.x, oldLocation.y);
        List<String> newTopics = index.getNearestNeighbors(newLocation.x, newLocation.y);
        if(!oldTopics.equals(newTopics)){
            callback.onSubscriptionChange(oldTopics, newTopics);
        }
    }
    
}
