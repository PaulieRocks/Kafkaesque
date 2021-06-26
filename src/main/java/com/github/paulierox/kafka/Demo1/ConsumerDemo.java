package com.github.paulierox.kafka.Demo1;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String groupid="group3";
        String bootstrapservers="127.0.0.1:9092";

        System.out.println("Hello Consumer World");

        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapservers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(properties);
        //subscribe consumer to our topics
        consumer.subscribe(Collections.singleton("first_topic"));
        //poll for new data
        while(true)
        {
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:records){
                logger.info("The key is : "+record.key());
                logger.info("The Value is : "+record.value());
                logger.info("Partition : "+record.partition());

            }
        }


    }
}