package com.practice.kafka.producer;

import com.practice.kafka.model.OrderModel;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.StringTokenizer;

public class OrderSerdProducer {
    public static  final Logger logger = LoggerFactory.getLogger(OrderSerdProducer.class);
    public static void main(String[] args) {

        String topicName = "order-serde-topic";

        Properties props = getProperties();

        //KafkaProducer Object Create
        KafkaProducer<String, OrderModel> kafkaProducer = new KafkaProducer<>(props);

        String filePath = "C:\\Kafka\\kafkaEx\\kafkapro01\\practice\\src\\main\\resources\\pizza_sample.txt";

        sendFileMessage(kafkaProducer, topicName, filePath);

        finish(kafkaProducer);
    }

    private static void finish(KafkaProducer<String, OrderModel> kafkaProducer) {
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
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());
        return props;
    }

    private static void sendFileMessage(KafkaProducer<String, OrderModel> kafkaProducer, String topicName, String path) {
        String line = "";
        final String delimiter = ",";
        try {
            FileReader fileReader = new FileReader(path);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            while((line = bufferedReader.readLine()) != null){
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                OrderModel orderModel = new OrderModel(tokens[1], tokens[2], tokens[3], tokens[4],
                        tokens[5],tokens[6], LocalDateTime.parse(tokens[7].trim(),formatter));
                sendMessage(kafkaProducer, topicName, key, orderModel);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, OrderModel> kafkaProducer, String topicName, String key, OrderModel value) {

        //ProducerRecord Object Create
        ProducerRecord<String, OrderModel> producerRecord = new ProducerRecord<>(topicName, key, value);

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
