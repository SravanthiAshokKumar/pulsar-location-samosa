package epl.pubsub.location.pulsarclient;

public interface SubscriptionChangedCallback<T>{

    void onSubscriptionChange(T oldVal, T newVal);
}
