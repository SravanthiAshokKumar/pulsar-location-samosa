# pulsar-location-samosa
Wrapper for Pulsar Client to handle subscription changes due to changes in location
## Introduction  
This library uses the `PulsarClient` APIs underneath to perform pub/sub related operations. Configurations are written in YAML. The library provides a `LocationManager` interface to monitor for changes in location. The `LocationSubscriptionHandler` interface allows for location changes to be translated to topic changes.   
## Building  
```shell  
cd /path/to/pulsar-location-samosa  
mvn compile  
mvn install  
```  
## Usage  
To create a consumer, first create a `PulsarLocationClient` instance. This needs a yaml file as input. The repo has an example config file  
``` java 

PulsarLocationClient client = PulsarLocationClientBuilder.getPulsarLocationClient("pulsar.yaml");  
PulsarLocationConsumer conusmer = client.getNewConsumer();  
consumer.start("testTopic", new MessageCallback({  
    public void onMessageReceived(Message<byte[]> message){    
      System.out.println("received message");  
    }  
  }  
```
To create a producer, follow a similar procedure  
```java  
PulsarLocationProducer producer = client.getNewProducer();  
producer.start("testTopic");  
producer.sendMessage(new String("test-message").getBytes();  
```   
Now, these handle the subscription changes due to location changes transparently. We still need to create a `LocationSubscriptionHandler` to translate location change to topic change. The Handler takes in a spatial index found [here](https://github.com/Manasvini/indexPerf). This can be done for the producer as follows:  
```java  
Index index = IndexFactory.getInitializedIndex(indexConfig.minX, indexConfig.minY, indexConfig.maxX, indexConfig.maxY, indexConfig.blockSize, IndexFactory.IndexType.GEOHASH, props);  
LocationSubscriptionHandlerSingleTopicImpl producerHandler = new LocationSubscriptionHandlerSingleTopicImpl(index);  
```


  
