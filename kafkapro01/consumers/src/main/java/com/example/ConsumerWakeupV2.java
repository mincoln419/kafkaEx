package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerWakeupV2 {

    public static  final Logger logger = LoggerFactory.getLogger(ConsumerWakeupV2.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.129:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-02");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topicName));

        //main thread get
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                logger.info("main thread is dead.. by calling wake...up...");
                kafkaConsumer.wakeup();
                try{
                    mainThread.join();
                }catch(InterruptedException ie){
                    logger.error(ie.getMessage());
                }
            }
        });

        int loopCnt = 0;

        try{

            while(true){
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000L));
                logger.info("##### loopCnt : {} consumerRecords count : {}", loopCnt++, consumerRecords.count());

                for(ConsumerRecord<String, String> record : consumerRecords){
                    logger.info("key : {}, record value: {}, partition : {}, record-offset ; {}"
                            , record.key(), record.value(), record.partition(), record.offset());
                }

                try{
                    logger.info("main threads is sleeping {} ms during while loop", loopCnt * 10000);
                    Thread.sleep(loopCnt * 10000);
                }catch (InterruptedException e){
                    logger.error(e.getMessage());
                }

            }
        }catch (WakeupException e){
            logger.error("wakeup exception has been called");
        }finally {
            logger.info("finally consumer is closing");
            kafkaConsumer.close();
        }
    }
}