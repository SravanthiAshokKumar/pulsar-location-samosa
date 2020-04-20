package epl.pubsub.location;

public interface LocationChangedCallback{

    void onLocationChange(Location oldLocation, Location newLocation);
}
