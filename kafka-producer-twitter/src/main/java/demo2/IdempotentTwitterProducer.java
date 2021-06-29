package demo2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class IdempotentTwitterProducer {
    private String bootstrapServer="127.0.0.1:9092";
    private String topic="twitter_feed";

    public IdempotentTwitterProducer(){}

    Logger logger= LoggerFactory.getLogger(IdempotentTwitterProducer.class.getName());
    public static void main(String[] args) {
        System.out.println("Hey Twitter Bird");
        new IdempotentTwitterProducer().run();
    }

    public void run(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //create a twitter client
        Client client=createTwitterClient(msgQueue);
        client.connect();

        //create a kafka producer

        KafkaProducer<String, String> producer= createProducer();
        //loop to send tweets to kafka

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null)
            {
                logger.info(msg);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,null,msg);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e!=null){
                            logger.error("Something Bad Happened",e);

                        }
                    }
                });
            }


        }
    }

    public KafkaProducer<String, String> createProducer()
    {
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        //Create Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
 //Setting the below properties explicitly. Not required as Idempotent Producers by default are set to the below values
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");




        KafkaProducer<String,String> producer=new KafkaProducer<String,String>(properties);
        return producer;

    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        String APIKey="c18injZU4zH8MTeMOzCNXF9rE";
        String APISecret="ZOr6DaUcEjNN4VEu5FHqyN2V0hdYGnxzScYXJWKqCi9B4HKWoH";
        String AccessToken="798288782-aT7KPNSiYRPVzdNTdc4dcwjw8R1yURaR08nVBl2u";
        String AccessSecret="waZOAoOPe8g7lmZXskoIc149FQL7fnbQGOKVN4pxogen3";


        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */




        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
                Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
                StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
                List<String> terms = Lists.newArrayList("bitcoin");
                hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
                Authentication hosebirdAuth = new OAuth1(APIKey,APISecret,AccessToken,AccessSecret);

                ClientBuilder builder = new ClientBuilder()
                        .name("Hosebird-Client-01")                              // optional: mainly for the logs
                        .hosts(hosebirdHosts)
                        .authentication(hosebirdAuth)
                        .endpoint(hosebirdEndpoint)
                        .processor(new StringDelimitedProcessor(msgQueue));                          // optional: use this if you want to process client events

                Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
                return hosebirdClient;
            }
}
