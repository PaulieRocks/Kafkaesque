import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class IdempotentElasticKafkaConsumer {

    public static RestHighLevelClient createClient()
    {

        String hostname="paulieelasticbox-5592070060.us-east-1.bonsaisearch.net";
        String username="pzrw5pblas";
        String password="utxl0va7cs";

        final CredentialsProvider credentialsProvider= new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));
        RestClientBuilder builder= RestClient.builder(new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client= new RestHighLevelClient(builder);
        return client;

    }

    public static void main(String[] args) throws IOException {
        Logger logger= LoggerFactory.getLogger(IdempotentElasticKafkaConsumer.class.getName());
        RestHighLevelClient client= createClient();
        String id;
        KafkaConsumer<String,String> consumer= createConsumer("twitter_feed");
        while(true)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:records){
                id=extractIdFromTweet(record.value());
                IndexRequest indexRequest= new IndexRequest("twitter", "tweets", id).source(record.value(), XContentType.JSON);
                IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
//                String id= indexResponse.getId();
                logger.info(id);


            }
        }


    }

    private static JsonParser jsonParser= new JsonParser();
    private static String extractIdFromTweet(String tweetJson)
    {
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
    public static KafkaConsumer<String, String> createConsumer(String topic)
    {
        String groupid="kafka-demo-elasticsearch";
        String bootstrapservers="127.0.0.1:9092";
        System.out.println("Hello Consumer World");
        //create Consumer Configs
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapservers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //create consumer
        org.apache.kafka.clients.consumer.KafkaConsumer<String,String> consumer= new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

}
