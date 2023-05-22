package com.practice.kafka.consumer;

import com.practice.kafka.producer.FileAppendProducer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class BaseConsumer<K , V> {

    public static  final Logger logger = LoggerFactory.getLogger(BaseConsumer.class);

    private KafkaConsumer<K, V> kafkaConsumer;

    private List<String> topics;

    public BaseConsumer(Properties props, List<String> topics) {
        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.topics = topics;
    }
    
    public void initConsumer(){
        this.kafkaConsumer.subscribe(this.topics);
        shutdownHookToRuntime(this.kafkaConsumer);
    }

    private void shutdownHookToRuntime(KafkaConsumer<K,V> kafkaConsumer) {
        //main thread
        Thread mainThread = Thread.currentThread();

        //main thread 종료시 thread로 KafkaConsumer wakup() 호출
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
    }

    private void processRecord(ConsumerRecord<K, V> record){
        logger.info("key : {}, record value: {}, partition : {}, record-offset ; {}"
                , record.key(), record.value(), record.partition(), record.offset());
    }

    public void pollConsumers(long durationMills, String commitMode){
        try{
            if("sync".equals(commitMode)){
                pollCommitSync(durationMills);
            }else{
                pollCommitAsync(durationMills);
            }

        }catch (WakeupException e){
            logger.error("wakeup exception has been called");
        }catch (Exception e){
            logger.error(e.getMessage());
        }finally {
            logger.info("finally consumer is closing");
            try{
                kafkaConsumer.commitSync();
            }catch (CommitFailedException ce){
                logger.error(ce.getMessage());
            }
            kafkaConsumer.close();
        }
    }

    private void pollCommitAsync(long durationMills ) throws  WakeupException, Exception{
        int loopCnt = 0;

        while(true){
            ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(durationMills);
            logger.info("##### loopCnt : {} consumerRecords count : {}", loopCnt++, consumerRecords.count());

            consumerRecords.forEach(record -> processRecord(record));

            kafkaConsumer.commitAsync((offset, e) -> {
                if(e != null){
                    logger.info("offset : {} exception : {}", offset, e.getMessage());
                }
            });
        }

    }

    public void pollCommitSync(long durationMills) throws  WakeupException, Exception{

        int loopCnt = 0;

        while(true){
            ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(durationMills);
            logger.info("##### loopCnt : {} consumerRecords count : {}", loopCnt++, consumerRecords.count());

            consumerRecords.forEach(record -> processRecord(record));

            try{
                if(consumerRecords.count() > 0){
                    kafkaConsumer.commitSync();
                    logger.info("kafka consumer sync commit!");
                }
            }catch(CommitFailedException e){
                logger.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        String topicName = "file-topic";
        Properties props = propSetting();

        BaseConsumer<String, String> baseConsumer = new BaseConsumer<>(props, List.of(topicName));

        baseConsumer.initConsumer();
        baseConsumer.pollConsumers(1000, "sync");
    }

    private static Properties propSetting() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.145.129:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-08");
        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        return props;
    }
}
