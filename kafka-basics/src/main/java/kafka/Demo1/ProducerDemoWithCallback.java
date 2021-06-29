package kafka.Demo1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerDemoWithCallback {

    public static void main(String[] args) throws InterruptedException{
        Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String server = "127.0.0.1:9092";
        System.out.println("Hello World");

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");



        for (int i = 509; i < 510; i++) {
            //create Producer

                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            //create Producer record

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", Integer.toString(i));

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
                    });
                producer.flush();
                producer.close();

        }
    }
}
