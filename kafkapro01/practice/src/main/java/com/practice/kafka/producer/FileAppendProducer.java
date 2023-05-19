package com.practice.kafka.producer;

import com.practice.kafka.event.EventHandler;
import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

public class FileAppendProducer {

    public static  final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class);
    public static void main(String[] args) {

        String topicName = "file-topic";

        Properties props = getProperties();

        //KafkaProducer Object Create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        File file = new File("C:\\Kafka\\kafkaEx\\kafkapro01\\practice\\src\\main\\resources\\pizza_append.txt");
        EventHandler eventHandler = new FileEventHandler(kafkaProducer, topicName, false);
        FileEventSource fileEventSource = new FileEventSource( file, eventHandler, 1000);

        Thread fileEventSourceThread = new Thread(fileEventSource);

        fileEventSourceThread.start();
        try{
            fileEventSourceThread.join();
        }catch(InterruptedException e){
            logger.error(e.getMessage());
        }
    }


    private static Properties getProperties() {
        Properties props = new Properties();

        //bootstrap.servers, key.serializer.class, value.serializer.class
        //props.setProperty("bootstrap.servers", "192.168.145.129:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.129:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

}
