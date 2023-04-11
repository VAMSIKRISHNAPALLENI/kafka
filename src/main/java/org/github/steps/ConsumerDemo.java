package org.github.steps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args){

        String bootstrap="127.0.0.1:9092";
        String groupID="the_originals";
        String topics="the_originals";
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());
        //Consumer Config
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //Consumer creation
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);
        //consumer Subscribe
        consumer.subscribe(Arrays.asList(topics));
        //Consumer Poll
        while(true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){
                logger.info("Key : "+record.key()+"\n value : "+record.value()+
                        "\n topic : "+record.topic()+"\n offset"+record.offset()+
                        "\n partition : "+record.partition());
            }
        }
    }
}
