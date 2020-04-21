package epl.pubsub.location;

import epl.pubsub.location.indexperf.Index;
import epl.pubsub.location.pulsarclient.SubscriptionChangedCallback;


public class LocationSubscriptionHandlerSingleTopicImpl implements  LocationSubscriptionHandler<String> {

    private Index index;
    private SubscriptionChangedCallback<String> callback;

    public LocationSubscriptionHandlerSingleTopicImpl(Index index){
        this.index = index;

    }
    public void initSubscriptionChangedCallback(SubscriptionChangedCallback<String> callback) {
        this.callback = callback;
    }
    
    @Override
    public void onLocationChange(Location oldLocation, Location newLocation){
        String oldTopic = index.getStringValue(oldLocation.x, oldLocation.y);
        String newTopic = index.getStringValue(newLocation.x, newLocation.y);
        if(!oldTopic.equals(newTopic)){
            callback.onSubscriptionChange(oldTopic, newTopic);
        }
    }
    
   
}
