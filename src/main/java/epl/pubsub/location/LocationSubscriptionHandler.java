package epl.pubsub.location;

import epl.pubsub.location.pulsarclient.SubscriptionChangedCallback;


public interface LocationSubscriptionHandler<T> extends LocationChangedCallback {

    void initSubscriptionChangedCallback(SubscriptionChangedCallback<T> callback); 

}
