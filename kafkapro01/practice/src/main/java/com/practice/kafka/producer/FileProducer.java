package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.StringTokenizer;
import java.util.stream.IntStream;

public class FileProducer {
    public static  final Logger logger = LoggerFactory.getLogger(FileProducer.class);
    public static void main(String[] args) {

        String topicName = "file-topic";

        Properties props = getProperties();

        //KafkaProducer Object Create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        String filePath = "C:\\Kafka\\kafkaEx\\kafkapro01\\practice\\src\\main\\resources\\pizza_sample.txt";

        sendFileMessage(kafkaProducer, topicName, filePath);

        finish(kafkaProducer);
    }

    private static void finish(KafkaProducer<String, String> kafkaProducer) {
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
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private static void sendFileMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String path) {
        String line = "";
        final String delimiter = ",";
        try {
            FileReader fileReader = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null){
                StringTokenizer st = new StringTokenizer(line, delimiter);
                String key = st.nextToken();
                StringBuffer value = new StringBuffer();
                while(true){
                    value.append(st.nextToken());
                    if(!st.hasMoreTokens()){
                        break;
                    }
                    value.append(delimiter);
                }
                sendMessage(kafkaProducer, topicName, key, value.toString());
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, String> kafkaProducer, String topicName, String key, String value) {

        //ProducerRecord Object Create
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);

        logger.info("key = {} value = {}", key, value);

        //KafkaProducer Message Send
        kafkaProducer.send(producerRecord, (m, e) -> {
            if(e == null){
                logger.info("\n ######callback########## record metadata received ####" +
                        "\n" + "partition : " + m.partition() +
                        "\n" + "offset : " + m.offset() +
                        "\n" + "timestamp : " + m.timestamp()
                );
            }else{
                logger.error(e.getMessage());
            }
        });

    }
}
