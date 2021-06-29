package kafka.Demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProducerDemoWithCallbackKeys {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        final Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallbackKeys.class);
        String server = "127.0.0.1:9092";
        System.out.println("Hello World");

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);



        for (int i = 10; i < 20; i++) {

                String topic="first_topic";
                String value="hello world"+Integer.toString(i);
                String key="id"+Integer.toString(i);
                System.out.println(key+" "+value);
            //create Producer record

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

                TimeUnit.SECONDS.sleep(1);
                //send data
                producer.send(record, new Callback() {

                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e==null){
                            logger.info("Received new Metadata"+"\n"+
                                    "Topic: "+recordMetadata.topic()+"\n"+
                                    "Partition: "+recordMetadata.partition()+"\n"+
                                    "Offset: "+ recordMetadata.offset()+"\n"+
                                    "Time Stamp"+recordMetadata.timestamp());
                        }
                        else{

                            logger.error(String.valueOf(e));

                        }

                        }
                    }).get();
//                producer.flush();
//                producer.close();

        }
    }
}
