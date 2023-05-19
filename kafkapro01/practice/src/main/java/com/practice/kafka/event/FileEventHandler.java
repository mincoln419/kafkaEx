package com.practice.kafka.event;

import com.practice.kafka.producer.FileProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler{

    public static  final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;
    private final boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);


        logger.info("key = {} value = {}", messageEvent.key, messageEvent.value);
        if(this.sync){
            RecordMetadata m =  kafkaProducer.send(producerRecord).get();
            logger.info("\n ######callback########## record metadata received ####" +
                    "\n" + "partition : " + m.partition() +
                    "\n" + "offset : " + m.offset() +
                    "\n" + "timestamp : " + m.timestamp()
            );

        }else{
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

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String topicName = "file-topic";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.129:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        boolean sync = true;
        EventHandler eventHandler = new FileEventHandler(producer, topicName, sync);
        MessageEvent messageEvent = new MessageEvent("key00001", "this is test message");
        eventHandler.onMessage(messageEvent);

    }
}
