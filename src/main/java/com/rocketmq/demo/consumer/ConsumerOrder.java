package com.rocketmq.demo.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ConsumerOrder {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_consumer_group");
        consumer.setNamesrvAddr("192.168.26.3:10911");
        //设置消息拉取上限
        consumer.setConsumeMessageBatchMaxSize(2);

        consumer.subscribe("Topic_Order_Demo",    //指定要消费的主题
                "*"  //过滤规则
        );
        consumer.setMessageListener(new MessageListenerOrderly() {
                                        @Override
                                        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                                            //读取消息
                                            for(MessageExt msg:list){
                                                //获取主题
                                                String topic = msg.getTopic();
                                                //获取标签
                                                String tags = msg.getTags();
                                                //获取信息
                                                byte[] body = msg.getBody();
                                                try {
                                                    String result = new String(body, RemotingHelper.DEFAULT_CHARSET);
                                                    System.out.println("Order---Consumer消费信息-----topic:"+topic+",tags:"+tags+",result:"+result);
                                                } catch (UnsupportedEncodingException e) {
                                                    e.printStackTrace();
                                                    //消息重试
                                                    return  ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                                                }
                                            }

                                            return ConsumeOrderlyStatus.SUCCESS;
                                        }
                                    }
        );
        //开启Consumer
        consumer.start();

    }
}
