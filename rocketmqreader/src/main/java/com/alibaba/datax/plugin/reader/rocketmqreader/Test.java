package com.alibaba.datax.plugin.reader.rocketmqreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.*;

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

            String[] columns = new String[]{
                    "order_service-order_base-order_id",
                    "order_service-order_base-order_from",
                    "order_service-order_base-order_no",
                    "order_service-order_base-category",
                    "order_service-order_base-serve_type"
            };

            for (MessageQueue mq : mqs) {

                System.out.println("Consume from the queue: " + mq);

                boolean flag = true;

                SINGLE_MQ:
                while (flag) {
                    try {
                        PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);

                        System.out.println(pullResult);
                        putMessageQueueOffset(mq, pullResult.getNextBeginOffset());

                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> msgs = pullResult.getMsgFoundList();
                                for (MessageExt msg : msgs) {
                                    String msgBody = new String(msg.getBody());
                                    JSONObject messageMap = null;
                                    try {
                                        messageMap = JSONObject.parseObject(msgBody);
                                    } catch (Throwable e) {
                                        System.out.println("msgBody parse error:" + msgBody);
                                        continue;
                                    }
                                    if (StringUtils.isEmpty(messageMap.getString("order_id"))
                                            || StringUtils.isEmpty(messageMap.getString("order_from"))
                                            || StringUtils.isEmpty(messageMap.getString("order_no"))
                                            || StringUtils.isEmpty(messageMap.getString("category"))
                                            || StringUtils.isEmpty(messageMap.getString("serve_type"))) {
                                        System.out.println("------------" + msgBody);
                                        continue;
                                    }
                                    System.out.println("msgBody:" + msgBody);
                                    Map<String, Object> returnMap = new HashMap<String, Object>();
                                    String database = messageMap.get("_db_") + "";
                                    String table = messageMap.get("_table_") + "";
                                    String event = messageMap.get("_event_") + "";
                                    messageMap.remove("_db_");
                                    messageMap.remove("_table_");
                                    messageMap.remove("_event_");

                                    for (Map.Entry<String, Object> entry : messageMap.entrySet()) {
                                        returnMap.put(database + "-" + table + "-" + entry.getKey(), entry.getValue());
                                    }

                                    ArrayList recordColume = new ArrayList();

                                    for (String column : columns) {
                                        if (StringUtils.isEmpty(returnMap.get(column) + "")) {
                                            recordColume.add(null);
                                            Column col = getColumn(column, returnMap.get(column));
//                                                record.addColumn(col);
                                        } else {
                                            assert returnMap.get(column) != null && !"".equals(returnMap.get(column));
                                            recordColume.add(returnMap.get(column) + "");
                                            Column col = getColumn(column, returnMap.get(column));
                                        }
                                    }

                                    System.out.println("before recordSender send!");
                                    flag = false;
                                    System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgBody + "%n");

                                }
                                break;
                            case NO_MATCHED_MSG:
                                break;
                            case NO_NEW_MSG:
                                System.out.println("NO_NEW_MSG");
                                break SINGLE_MQ;
                            case OFFSET_ILLEGAL:
                                break;
                            default:
                                break;
                        }

                    } catch (Throwable e) {
                        e.printStackTrace();
                    } finally {
                    }
                }
            }
            consumer.shutdown();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private static Column getColumn(String key, Object value) {
        if (value == null) {
            return null;
        }
        Column col = null;
        if (value instanceof Long) {
            col = new LongColumn((Long) value);
        } else if (value instanceof Integer) {
            col = new LongColumn(((Integer) value).longValue());
        } else if (value instanceof Byte) {
            col = new LongColumn(((Byte) value).longValue());
        } else if (value instanceof Short) {
            col = new LongColumn(((Short) value).longValue());
        } else if (value instanceof String) {
            col = new StringColumn((String) value);
        } else if (value instanceof Double) {
            col = new DoubleColumn((Double) value);
        } else if (value instanceof Float) {
            col = new DoubleColumn(((Float) value).doubleValue());
        } else if (value instanceof Date) {
            col = new DateColumn((Date) value);
        } else if (value instanceof Boolean) {
            col = new BoolColumn((Boolean) value);
        } else if (value instanceof byte[]) {
            col = new BytesColumn((byte[]) value);
        } else {
            throw DataXException.asDataXException(RocketmqReaderErrorCode.ILLEGAL_VALUE, ":" + ",key:" + key);
        }
        return col;
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
