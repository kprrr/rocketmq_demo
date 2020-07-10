package com.rocketmq.demo.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class BroadcastingConsumerB {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_consumer_broadcasting_group");
        consumer.setNamesrvAddr("192.168.40.129:9876");
        //设置消息拉取上限
        consumer.setConsumeMessageBatchMaxSize(2);

        //默认是集群消费模式,改成广播模式
        consumer.setMessageModel(MessageModel.BROADCASTING);


        consumer.subscribe("Topic_Broadcasting_Demo",    //指定要消费的主题
                            "*"  //过滤规则
                             );
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //迭代消息信息
                for (MessageExt msg : list) {
                    //获取主题
                    String topic = msg.getTopic();
                    //获取标签
                    String tags = msg.getTags();
                    //获取信息
                    byte[] body = msg.getBody();
                    try {
                        String result = new String(body, RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("B--------Consumer消费信息-----topic:"+topic+",tags:"+tags+",result:"+result);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        //消息消费重试
                        return  ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                //6.返回消息读取状态
                //消息消费完成
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //开启Consumer
        consumer.start();

    }
}
