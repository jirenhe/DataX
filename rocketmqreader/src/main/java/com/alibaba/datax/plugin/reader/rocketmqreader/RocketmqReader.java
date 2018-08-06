package com.alibaba.datax.plugin.reader.rocketmqreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RocketmqReader extends Reader {

    public static class Job extends Reader.Job {

        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;
        private DefaultMQPushConsumer mqConsumer;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            // warn: 忽略大小写
//            dealColumn(this.originalConfig);

//            Long sliceRecordCount = this.originalConfig
//                    .getLong(Key.SLICE_RECORD_COUNT);
//            if (null == sliceRecordCount) {
//                throw DataXException.asDataXException(RocketmqReaderErrorCode.REQUIRED_VALUE,
//                        "没有设置参数[sliceRecordCount].");
//            } else if (sliceRecordCount < 1) {
//                throw DataXException.asDataXException(RocketmqReaderErrorCode.ILLEGAL_VALUE,
//                        "参数[sliceRecordCount]不能小于1.");
//            }
            try {
//                this.mqConsumer = new DefaultMQPushConsumer("example_group_name");
//
//                this.mqConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//                this.mqConsumer.setNamesrvAddr("192.168.0.159:9876");
//                this.mqConsumer.subscribe("test111", "*");


            } catch (Exception e) {

            }


        }

        private void dealColumn(Configuration originalConfig) {
            List<JSONObject> columns = originalConfig.getList(Key.COLUMN,
                    JSONObject.class);
            if (null == columns || columns.isEmpty()) {
                throw DataXException.asDataXException(RocketmqReaderErrorCode.REQUIRED_VALUE,
                        "没有设置参数[column].");
            }

            List<String> dealedColumns = new ArrayList<String>();
            for (JSONObject eachColumn : columns) {
                Configuration eachColumnConfig = Configuration.from(eachColumn);


                String typeName = eachColumnConfig.getString(Constant.TYPE);
                if (StringUtils.isBlank(typeName)) {
                    // empty typeName will be set to default type: string
                    eachColumnConfig.set(Constant.TYPE, Type.STRING);
                } else {
                    if (Type.DATE.name().equalsIgnoreCase(typeName)) {
                        boolean notAssignDateFormat = StringUtils
                                .isBlank(eachColumnConfig
                                        .getString(Constant.DATE_FORMAT_MARK));
                        if (notAssignDateFormat) {
                            eachColumnConfig.set(Constant.DATE_FORMAT_MARK,
                                    Constant.DEFAULT_DATE_FORMAT);
                        }
                    }
                    if (!Type.isTypeIllegal(typeName)) {
                        throw DataXException.asDataXException(
                                RocketmqReaderErrorCode.NOT_SUPPORT_TYPE,
                                String.format("不支持类型[%s]", typeName));
                    }
                }
                dealedColumns.add(eachColumnConfig.toJSON());
            }

            originalConfig.set(Key.COLUMN, dealedColumns);
        }


        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();

            for (int i = 0; i < adviceNumber; i++) {
                configurations.add(this.originalConfig.clone());
            }
            return configurations;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Reader.Task {

        private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();


        private static Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;

        private List<String> columns;

        private long sliceRecordCount;

        private boolean haveMixupFunction;
//
//        private DefaultMQPushConsumer mqConsumer;


        @Override
        public void init() {
            LOG.info("rocketmqreader start init ");
            this.readerSliceConfig = super.getPluginJobConf();
            this.columns = this.readerSliceConfig.getList(Key.COLUMN,
                    String.class);

            this.sliceRecordCount = this.readerSliceConfig.getLong(Key.SLICE_RECORD_COUNT);
            this.haveMixupFunction = this.readerSliceConfig.getBool(Constant.HAVE_MIXUP_FUNCTION, false);

//            try {
//                this.mqConsumer = new DefaultMQPushConsumer("example_group_name");
//
//                this.mqConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//                this.mqConsumer.setNamesrvAddr("192.168.0.159:9876");
//                this.mqConsumer.subscribe("test111", "*");
//            } catch (Exception e) {
//            }

            LOG.info("rocketmqreader end init ");
        }


        public static void runRocketmq() {

        }

        @Override
        public void prepare() {

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


        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("rocketmqreader start read ");

            try {
                DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("example_group_name");
                consumer.setNamesrvAddr("192.168.0.159:9876");
                consumer.start();
                Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("test111");

                for (MessageQueue mq : mqs) {

                    System.out.println("Consume from the queue: " + mq);

                    boolean flag = true;

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

                                        LOG.info("mq msgbody:" + msgBody);
                                        ArrayList recordColume = new ArrayList();
                                        Record record = recordSender.createRecord();

                                        for (String column : columns) {
                                            if (StringUtils.isEmpty(returnMap.get(column) + "")) {
                                                recordColume.add(null);
                                                Column col = getColumn(column, returnMap.get(column));
//                                                record.addColumn(col);
                                            } else {
                                                recordColume.add(returnMap.get(column) + "");
                                                Column col = getColumn(column, returnMap.get(column));
                                                record.addColumn(col);
                                            }
                                        }

                                        System.out.println("before recordSender send!");
                                        System.out.println("record is :" + record);
                                        recordSender.sendToWriter(record);
                                        flag = false;
                                        recordSender.terminate();
                                        System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgBody + "%n");

                                    }
                                    break;
                                case NO_MATCHED_MSG:
                                    break;
                                case NO_NEW_MSG:
                                    System.out.println("NO_NEW_MSG");
                                case OFFSET_ILLEGAL:
                                    break;
                                default:
                                    break;
                            }

                        } catch (Throwable e) {
                            e.printStackTrace();
                            LOG.error("", e);
                        } finally {
                        }
                        Thread.sleep(2000);
                    }
                }
                System.out.println("shutdown ------");
                consumer.shutdown();
            } catch (Throwable e) {
                e.printStackTrace();
                LOG.error("", e);
            }

//            try {


//                String msgBody = "{\"_db_\":\"order_service\",\"_table_\":\"order_base\",\"_event_\":\"row_insert\",\"order_id\":\"12311111\",\"order_from\":\"rocketmq\",
// \"order_no\":\"P12311111\",\"category\":\"1\",\"serve_type\":\"2\",\"account_id\":\"\",\"account_type\":\"\",\"order_status\":\"\",\"region_id\":\"\",\"appoint_method\":\"\",\"sp_type\":\"\",
// \"order_type\":\"\",\"is_delete\":\"\",\"refund_status\":\"\",\"order_pay_status\":\"\",\"is_logistics\":\"\",\"repair_type\":\"\",\"is_return\":\"\",\"create_time\":\"\",\"update_time\":\"\"}";
//                Map<String, Object> returnMap = new HashMap<String, Object>();
//                Map messageMap = JSON.parseObject(msgBody);
//                String database = messageMap.get("_db_") + "";
//                String table = messageMap.get("_table_") + "";
//                String event = messageMap.get("_event_") + "";
//                messageMap.remove("_db_");
//                messageMap.remove("_table_");
//                messageMap.remove("_event_");
//
//                Iterator<Map.Entry<Integer, Integer>> entries = messageMap.entrySet().iterator();
//                while (entries.hasNext()) {
//                    Map.Entry<Integer, Integer> entry = entries.next();
//                    returnMap.put(database + "-" + table + "-" + entry.getKey(), entry.getValue());
//                }
//
//                LOG.info("mq msgbody:" + msgBody);
//                ArrayList<String> recordColume = new ArrayList<String>();
//                Record record = recordSender.createRecord();
//
//                for (String column : columns) {
//                    if (StringUtils.isEmpty(returnMap.get(column) + "")) {
//                        recordColume.add(null);
//                        Column col = getColumn(column, returnMap.get(column));
//                        record.addColumn(col);
//                    } else {
//                        recordColume.add(returnMap.get(column) + "");
//                        LOG.info("recordColume:" + returnMap.get(column) + "");
//
//                        Column col = getColumn(column, returnMap.get(column));
//                        record.addColumn(col);
//
//                    }
//                    LOG.info("recordColumn:" + column);
//
//                }
//                LOG.info("recordSender:" + recordSender);
//                recordSender.sendToWriter(record);
//
//                System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgBody + "%n");
//
//                DefaultMQPushConsumer mqConsumer = new DefaultMQPushConsumer("example_group_name");
//
//                mqConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//                mqConsumer.setNamesrvAddr("192.168.0.159:9876");
//                mqConsumer.subscribe("test111", "*");
//
//                mqConsumer.registerMessageListener(new MessageListenerOrderly() {
//
//
//                    AtomicLong consumeTimes = new AtomicLong(0);
//
//                    @Override
//                    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
//                        context.setAutoCommit(false);
//
//                        for (MessageExt msg : msgs) {
//                            String msgBody = new String(msg.getBody());
//                            Map<String, Object> returnMap = new HashMap<String, Object>();
//                            Map messageMap = JSON.parseObject(msgBody);
//                            String database = messageMap.get("_db_") + "";
//                            String table = messageMap.get("_table_") + "";
//                            String event = messageMap.get("_event_") + "";
//                            messageMap.remove("_db_");
//                            messageMap.remove("_table_");
//                            messageMap.remove("_event_");
//
//                            Iterator<Map.Entry<Integer, Integer>> entries = messageMap.entrySet().iterator();
//                            while (entries.hasNext()) {
//                                Map.Entry<Integer, Integer> entry = entries.next();
//                                returnMap.put(database + "-" + table + "-" + entry.getKey(), entry.getValue());
//                            }
//
//                            LOG.info("mq msgbody:" + msgBody);
//                            ArrayList recordColume = new ArrayList();
//                            Record record = recordSender.createRecord();
//
//                            for (String column : columns) {
//                                if (StringUtils.isEmpty(returnMap.get(column) + "")) {
//                                    recordColume.add(null);
//                                    Column col = getColumn(column, returnMap.get(column));
//                                    record.addColumn(col);
//                                } else {
//                                    recordColume.add(returnMap.get(column) + "");
//                                    Column col = getColumn(column, returnMap.get(column));
//                                    record.addColumn(col);
//                                }
//                            }
//                            recordSender.sendToWriter(record);
//
//                            System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgBody + "%n");
//
//                        }
//                        this.consumeTimes.incrementAndGet();
//
//                        return ConsumeOrderlyStatus.SUCCESS;
//
//                    }
//                });
//                LOG.info("rocketmqreader fffffffff  ");
//
//                mqConsumer.start();
//            } catch (Exception e) {
//                LOG.info("rocketmqreader exception  " + e.getMessage());
//
//            }


            LOG.info("rocketmqreader end read ");
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }


        private Column getColumn(String key, Object value) {
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
    }

    private enum Type {
        STRING, LONG, BOOL, DOUBLE, DATE, BYTES,;

        private static boolean isTypeIllegal(String typeString) {
            try {
                Type.valueOf(typeString.toUpperCase());
            } catch (Exception e) {
                return false;
            }

            return true;
        }
    }


    public static void main(String[] args) {
        String json = "{\"_db_\":\"order_service\",\"_table_\":\"order_base\",\"_event_\":\"row_insert\",\"order_id\":\"1231111112\",\"order_from\":\"rocketmq\",\"order_no\":\"P12311111\"," +
                "\"category\":\"1\",\"serve_type\":\"2\",\"account_id\":\"\",\"account_type\":\"\",\"order_status\":\"\",\"region_id\":\"\",\"appoint_method\":\"\",\"sp_type\":\"\"," +
                "\"order_type\":\"\",\"is_delete\":\"\",\"refund_status\":\"\",\"order_pay_status\":\"\",\"is_logistics\":\"\",\"repair_type\":\"\",\"is_return\":\"\",\"create_time\":\"\"," +
                "\"update_time\":\"\"}";

        Map<String, Object> returnMap = new HashMap<String, Object>();
        Map messageMap = JSON.parseObject(json);
        String database = messageMap.get("_db_") + "";
        String table = messageMap.get("_table_") + "";
        String event = messageMap.get("_event_") + "";
        messageMap.remove("_db_");
        messageMap.remove("_table_");
        messageMap.remove("_event_");

        Iterator<Map.Entry<Integer, Integer>> entries = messageMap.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<Integer, Integer> entry = entries.next();
            returnMap.put(database + "-" + table + "-" + entry.getKey(), entry.getValue());
        }


    }

}
