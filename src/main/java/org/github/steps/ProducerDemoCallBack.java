package org.github.steps;

import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import java.util.Properties;

public class ProducerDemoCallBack {
    public static void main(String[] args){

        final Logger logger = LoggerFactory.getLogger(ProducerDemoCallBack.class);
        //Properties Defining
        String bootstrap="127.0.0.1:9092";
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
        for(int i=0;i<10;i++){
            //Producer record
            ProducerRecord<String,String> record= new ProducerRecord<String, String>("the_originals","Hakunna Mattata : " + Integer.toString(i));
            //send Data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time sending the data
                    if(e==null){
                        //executes when successfully sent to producer
                        logger.info("Receveied Metadata : "+"\n Topic : "+recordMetadata.topic()+
                                    "\n Partition : " +recordMetadata.partition()+
                                    "\n Offset : "     +recordMetadata.offset()+
                                    "\n TimeStamp : "+recordMetadata.timestamp());
                    }
                    else{
                        //executes when data is unsuccessful
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        //Flush the producer
        producer.flush();
        //Flush and close it
        producer.close();
    }
}
