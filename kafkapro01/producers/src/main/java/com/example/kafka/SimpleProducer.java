package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducer {
    public static  final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class);
    public static void main(String[] args) {

        String topicName = "simple-topic";

        //kafkaProducer configuration setting
        // null, "hello world"

        Properties props = new Properties();

        //bootstrap.servers, key.serializer.class, value.serializer.class
        //props.setProperty("bootstrap.servers", "192.168.145.129:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.129:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //KafkaProducer Object Create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        //ProducerRecord Object Create
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world2");

        //KafkaProducer Message Send
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
