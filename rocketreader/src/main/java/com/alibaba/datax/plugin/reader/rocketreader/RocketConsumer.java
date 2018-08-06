package com.alibaba.datax.plugin.reader.rocketreader;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.atomic.AtomicBoolean;

public class RocketConsumer implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketConsumer.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private String bootstrapServers;
    private DefaultMQPushConsumer mqConsumer;


    public static RocketConsumer using() {
        return new RocketConsumer();
    }




    private RocketConsumer() {
        mqConsumer = new DefaultMQPushConsumer("example_group_name");

        mqConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        mqConsumer.setNamesrvAddr("192.168.0.159:9876");
    }




    @Override
    public void run() {
        try {
            mqConsumer.subscribe("test111", "*");


        } catch (MQClientException e) {

        } finally {
            mqConsumer.shutdown();
        }
    }

    AtomicBoolean started = new AtomicBoolean(false);

    public void runAsThread() {
        if (started.get()) return;
        started.set(true);

        Thread dm = new Thread(this);
        dm.setDaemon(true);
        dm.setName("rocketmq-client");
        dm.start();
    }



    public void shutdown() {
        if (!closed.get()) {
            closed.set(true);
            mqConsumer.resume();
        }
    }

}
