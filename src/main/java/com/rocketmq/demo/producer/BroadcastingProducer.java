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
import java.util.ArrayList;
import java.util.List;

@Component
public class BroadcastingProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        System.out.println("进入producer-------------");
        DefaultMQProducer producer = new DefaultMQProducer("demo_BroadcastingProducer_group");
        producer.setNamesrvAddr("192.168.40.129:9876");
        producer.start();

        List<Message> messages =new ArrayList<>();
        for(int i=0;i<10;i++) {
            Message msg = new Message("Topic_Broadcasting_Demo",
                    "tags",
                    "keys"+i,
                    ("hello!"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            messages.add(msg);
        }


        SendResult result = producer.send(messages);
        System.out.println(result);
        producer.shutdown();

    }
}
