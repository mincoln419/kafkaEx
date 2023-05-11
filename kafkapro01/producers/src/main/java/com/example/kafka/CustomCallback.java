package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    public static  final Logger logger = LoggerFactory.getLogger(CustomCallback.class);

    private int index;

    public CustomCallback(int index){
        this.index = index;
    }

    @Override
    public void onCompletion(RecordMetadata m, Exception e) {
        if(e == null){
            logger.info("index = {} partition = {} offset = {} timestamp = {}", index, m.partition(),  m.offset(), m.timestamp());

        }else{
            e.printStackTrace();
        }
    }
}
