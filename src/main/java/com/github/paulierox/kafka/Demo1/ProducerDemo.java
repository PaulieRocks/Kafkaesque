package com.github.paulierox.kafka.Demo1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException{
        String server = "127.0.0.1:9092";
        System.out.println("Hello World");

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");



        for (int i = 500; i < 510; i++) {
            //create Producer

                KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

            //create Producer record

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", Integer.toString(i));

                TimeUnit.SECONDS.sleep(1);
                //send data
                producer.send(record);
                producer.flush();
                producer.close();

        }
    }
}
