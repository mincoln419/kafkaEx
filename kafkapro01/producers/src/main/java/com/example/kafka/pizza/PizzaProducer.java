package com.example.kafka.pizza;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {
    public static  final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);
    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = getProperties();

        //KafkaProducer Object Create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        sendPizzaMessage(topicName, kafkaProducer, 100 , 500 , 1000, 5, false);

        finish(kafkaProducer);
    }

    private static void sendPizzaMessage(String topicName, KafkaProducer<String, String> kafkaProducer
            , int iterCount, int interIntervalMillis, int intervalMills, int intervalCount, boolean sync) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2023;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while(iterSeq++ != iterCount) {
            Map<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);

            if(sync){
                RecordMetadata recordMetadata = sendKafkaMessageSync(topicName, kafkaProducer, pMessage.get("key"), pMessage.get("message"));
            }else{
                sendKafkaMessageAsync(topicName, kafkaProducer, pMessage.get("key"), pMessage.get("message"));
            }

            intervalProcess(intervalMills, intervalCount, iterSeq);

            interIntervalProcess(interIntervalMillis);
        }
    }

    private static void interIntervalProcess(int interIntervalMillis) {
        if(interIntervalMillis > 0){
            try {
                logger.info("##### InterIntervalMillis : {} #####", interIntervalMillis);
                Thread.sleep(interIntervalMillis);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private static void intervalProcess(int intervalMills, int intervalCount, int iterSeq) {
        if(intervalCount > 0 && (iterSeq % intervalCount) == 0){
            try {
                logger.info("##### IntervalCount: {} intervalMillis : {} #####", intervalCount, intervalMills);
                Thread.sleep(intervalMills);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
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
//        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6"); //5 이상이면 idempotence 로 가지 않는다.
//        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "50000");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000");
//        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        return props;
    }

    private static void sendKafkaMessageAsync(String topicName, KafkaProducer<String, String> kafkaProducer, String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);

        //KafkaProducer Message Send
        kafkaProducer.send(producerRecord, (m, e) -> {
            if(e == null){
                logger.info("Async key : {} message : {} partition : {} offset : {}", key, message, m.partition(), m.offset());
            }else{
                e.printStackTrace();
            }
        });
    }

    private static RecordMetadata sendKafkaMessageSync(String topicName, KafkaProducer<String, String> kafkaProducer, String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);

        //KafkaProducer Message Send
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord, (m, e) -> {
                if(e == null){
                    logger.info("Sync key : {} message : {} partition : {} offset : {}", key, message, m.partition(), m.offset());
                }else{
                    e.printStackTrace();
                }
            }).get();
            return recordMetadata;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

    }
}
