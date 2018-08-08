package com.alibaba.datax.plugin.reader.rocketmqreader;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * Created by Administrator on 2018/7/18.
 */
public class OrderProducer {

    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new
                DefaultMQProducer("producer1");
        //Launch the instance.
        producer.setNamesrvAddr("192.168.0.159:9876");

        producer.start();
        for (int i = 0; i < 2000; i++) {
            //Create a message instance, specifying topic, tag and message body.
            Message msg = new Message("test111" /* Topic */,
                    "TagA" /* Tag */,
                    ("{\"_db_\":\"order_service\",\"_table_\":\"order_base\",\"_event_\":\"row_insert\",\"order_id\":\"1" + i +"\",\"order_from\":\"rocketmq\",\"order_no\":\"P12311111\"," +
                            "\"category\":\"1\",\"serve_type\":\"2\",\"account_id\":\"\",\"account_type\":\"\",\"order_status\":\"\",\"region_id\":\"\",\"appoint_method\":\"\",\"sp_type\":\"\",\"order_type\":\"\",\"is_delete\":\"\",\"refund_status\":\"\",\"order_pay_status\":\"\",\"is_logistics\":\"\",\"repair_type\":\"\",\"is_return\":\"\",\"create_time\":\"\",\"update_time\":\"\"}").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            //Call send message to deliver message to one of brokers.
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        //Shut down once the producer instance is not longer in use.
        producer.shutdown();
    }
}