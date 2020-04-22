package epl.pubsub.location.pulsarclient;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Collections;

import com.google.common.collect.Sets;

class PulsarLocationClientImpl implements PulsarLocationClient {

    private PulsarClient client;
    private PulsarAdmin adminClient;
    
    private PulsarConfig config;
    
    @Override
    public void initClient(PulsarConfig config){
        this.config = config;

        ClientBuilder clientBuilder = PulsarClient.builder()
                                    .ioThreads(config.numIOThreads)
                                    .connectionsPerBroker(config.connectionsPerBroker)
                                    .serviceUrl(config.serviceUrl)
                                    .maxConcurrentLookupRequests(config.maxConcurrentLookups)
                                    .maxLookupRequests(config.maxLookups)
                                    .listenerThreads(config.listenerThreads);
        try {                        
            client = clientBuilder.build();

            PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(config.httpUrl);
            adminClient = pulsarAdminBuilder.build();

            try {
                String tenant = config.namespace.split("/")[0];
                String cluster = config.cluster;
                if(!adminClient.tenants().getTenants().contains(tenant)){
                    adminClient.tenants().createTenant(tenant, new TenantInfo(Collections.emptySet(), Sets.newHashSet(cluster)));
                }
                adminClient.namespaces().createNamespace(config.namespace);
            } catch(ConflictException ex){
                
            } catch(PulsarAdminException e){
                e.printStackTrace();
                System.out.println(e.getMessage());
            } 
        } catch(PulsarClientException ex){
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public PulsarLocationProducer getNewProducer(){
        return new PulsarLocationProducerImpl(client, config.producerConfig);
    }    
    
    @Override
    public PulsarLocationConsumer getNewConsumer(){
        return new PulsarLocationConsumerImpl(client);
    }

    @Override
    public void createTopics(List<String> topics){
        if(config.partitions ==1){
            return;
        }
        try {
            for(String topic: topics){
                
                adminClient.topics().createPartitionedTopic(topic, config.partitions);
            }
        } catch(PulsarAdminException ex){
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }

    @Override
    public String getTopicNamePrefix(){
        return config.topicType+"://" + config.namespace;
    }

}
