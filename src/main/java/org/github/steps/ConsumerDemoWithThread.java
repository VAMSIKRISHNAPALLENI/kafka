package org.github.steps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
/*
 * Just see how consumer works
 * */
public class ConsumerDemoWithThread {
    public static void main(String[] args){
        new ConsumerDemoWithThread().run();
    }
private  ConsumerDemoWithThread () {

}
private void run() {
    Logger logger= LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
    String bootstrap="127.0.0.1:9092";
    String groupID="my-sixth-application";
    String topics="the_originals";

    //Deals with mutliple threads
    CountDownLatch countDownLatch= new CountDownLatch(1);

    //Creating the consumer Runnable
    logger.info("Creating the consumer Thread");
    Runnable myConsumerRunnable= new ConsumerRunnable(bootstrap,groupID,topics,countDownLatch);

    //Starting the thread
    Thread myThread= new Thread(myConsumerRunnable);
    myThread.start();

    //add a shut down hook
    Runtime.getRuntime().addShutdownHook(new Thread(()->{
        logger.info("caught shut down hook");
                ((ConsumerRunnable) myConsumerRunnable).shutdown();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Application got exited");
    }
    ));
    try {
        countDownLatch.await();
    }
    catch (InterruptedException e){
        logger.error("Application got inturrpted",e);
    }
    finally {
        logger.info("Application is closing");
    }
}
    public class ConsumerRunnable implements Runnable{
       private CountDownLatch countDownLatch;
       private Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class.getName());
       private KafkaConsumer<String,String> consumer;
        private String topics ;

        //That is created thread from con
        public ConsumerRunnable(String bootstrap,String groupID, String topics,CountDownLatch countDownLatch){
            this.countDownLatch=countDownLatch;
            this.topics=topics;

            //Consumer Config
            Properties properties= new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //Consumer creation
            consumer= new KafkaConsumer<>(properties);

            //consumer Subscribe
            consumer.subscribe(Arrays.asList(topics));
        }


        @Override
        public void run() {
            try {
            //Consumer Poll for new data
            while(true){
                ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String,String> record: records){
                    logger.info("Key : "+record.key()+"\n value : "+record.value()+
                            "\n topic : "+record.topic()+"\n offset"+record.offset()+
                            "\n partition : "+record.partition());
                }
            }
            } catch (WakeupException w){
                logger.info("consumer poll is interrupted : ");
            }
            finally {
                consumer.close();
                //Tells our main code that we're done with consumer
                countDownLatch.countDown();
            }

        }
        //Used to shut down CcnsumerThread
        public void shutdown(){
            /*
            *Wakeup method is a special method to interrupt the consumer.poll()
            * it will throw a exception(WakeupException) when it is interrupted
            */
            consumer.wakeup();
        }
    }
}

