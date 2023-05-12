package com.example.kafka.pizza.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    private String specialKeyName;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfoList.size();
        int numSpecialPartitions = (int) (numPartitions * 0.5);

        if (keyBytes == null) {
            //return stickyPartitionCache.partition(topic, cluster);
            throw new InvalidRecordException("key should not be null");
        }

        int partitionIndex = 0;
        //"P001"만 5개의 partition중에 2개를 쓰고 나머지는 3개를 나눠서 씀
        if (String.valueOf(key).equals(specialKeyName)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        }else{
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }

        logger.info("key : {} is sent to partitions {}", key, partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }
}
