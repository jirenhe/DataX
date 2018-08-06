package com.alibaba.datax.plugin.reader.rocketmqreader;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Title
 * Author jirenhe@wanshifu.com
 * Time 2018/8/6.
 * Version v1.0
 */
public class Test {

    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) {
        try {
            DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("example_group_name");
            consumer.setNamesrvAddr("192.168.0.159:9876");
            consumer.start();
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("test111");

            for (MessageQueue mq : mqs) {

                System.out.println("Consume from the queue: " + mq);

                SINGLE_MQ:
                while (true) {
                    try {
                        PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);

                        System.out.println(pullResult);
                        putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> msgs = pullResult.getMsgFoundList();
                                for (MessageExt msg : msgs) {
                                    String msgBody = new String(msg.getBody());
                                    Map messageMap = null;
                                    try {
                                        messageMap = JSON.parseObject(msgBody);
                                    } catch (Throwable e) {
                                        continue;
                                    }
                                    System.out.println("msgBody:" + msgBody);
                                    int i = 1 / 0;
                                    System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgBody + "%n");

                                }
                                break;
                            case NO_MATCHED_MSG:
                                break;
                            case NO_NEW_MSG:
                                break SINGLE_MQ;
                            case OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }

                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
            consumer.shutdown();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {

        offseTable.put(mq, offset);

    }

    private static long getMessageQueueOffset(MessageQueue mq) {

        Long offset = offseTable.get(mq);

        if (offset != null)

            return offset;

        return 0;

    }
}
