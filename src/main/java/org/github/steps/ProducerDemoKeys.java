package org.github.steps;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        //Properties Defining
        String bootstrap="127.0.0.1:9092";
        Properties properties= new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
        for(int i=0;i<10;i++){
            String topic = "the_originals";
            String value = "Hello World"+Integer.toString(i);
            String key ="Id_"+Integer.toString(i);
            logger.info("key : "+key);
            //Producer record
            ProducerRecord<String,String> record= new ProducerRecord<String, String>(topic,key,value);
            //send Data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time sending the data
                    /*id-par
                    * 0-2
                    * 1-1
                    * 2-2
                    * 3-0
                    * 4-1
                    * 5-2
                    * 6-0
                    * 7-2
                    * 8-1
                    * 9-0
                    *
                    * */
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
            }).get();
        }
        //Flush the producer
        producer.flush();
        //Flush and close it
        producer.close();
    }
}
