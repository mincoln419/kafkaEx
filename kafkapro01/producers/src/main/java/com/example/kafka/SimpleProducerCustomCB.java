package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class SimpleProducerCustomCB {
    public static  final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class);
    public static void main(String[] args) {

        String topicName = "multipart-num-topic";

        Properties props = getProperties();

        //KafkaProducer Object Create
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        IntStream.range(0, 20).forEach(i -> {
            sendKafkaMessage(topicName, kafkaProducer , i);
        });

        finish(kafkaProducer);
    }

    private static void finish(KafkaProducer<Integer, String> kafkaProducer) {
        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        kafkaProducer.close();
        logger.info("################main thread end");
    }

    private static Properties getProperties() {
        Properties props = new Properties();

        //bootstrap.servers, key.serializer.class, value.serializer.class
        //props.setProperty("bootstrap.servers", "192.168.145.129:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.129:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static void sendKafkaMessage(String topicName, KafkaProducer<Integer, String> kafkaProducer, int index) {
        //ProducerRecord Object Create
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, index, "hello world " + index);

        Callback callback = new CustomCallback(index);
        //KafkaProducer Message Send
        kafkaProducer.send(producerRecord, callback);
    }
}
