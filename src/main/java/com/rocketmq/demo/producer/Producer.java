package com.rocketmq.demo.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;


/**
 * 生产者
 * 1.普通消息
 * 2.延迟消息
 */
@Component
public class Producer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        System.out.println("进入producer-------------");
        DefaultMQProducer producer = new DefaultMQProducer("demo_prodecer_group");
        producer.setNamesrvAddr("192.168.26.3:10911");
        producer.start();

        Message msg = new Message("Topic_Demo",
                                    "tags",
                                    "keys_1",
                ("hello!"+ LocalDateTime.now()).getBytes(RemotingHelper.DEFAULT_CHARSET));

        //设置延迟
        //RcoketMQ的延时等级为：1s，5s，10s，30s，1m，2m，3m，4m，
        // 5m，6m，7m，8m，9m，10m，20m，30m，1h，2h。level=0，表示不延时。
        // level=1，表示 1 级延时，对应延时 1s。level=2 表示 2 级延时，对应5s，以此类推。
        msg.setDelayTimeLevel(1);//1代表等级，不是1秒

        SendResult result = producer.send(msg);
        System.out.println(result);
        producer.shutdown();


    }
}
