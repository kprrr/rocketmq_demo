package com.rocketmq.demo.producer;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;

@Component

/**
 * 有序消息
 * 所谓的有序消息，就是手动指定队列ID
 *
 */

public class ProducerOrder {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建一个生产者,需要指定Producer的分组，
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("Group-Producer-1");
        //2.设置命名服务的地址,默认是去读取conf文件下的配置文件 rocketmq.namesrv.addr
        defaultMQProducer.setNamesrvAddr("192.168.26.128:9876");

        try{

            //3.启动生产者
            defaultMQProducer.start();
            int  orderId =0;
            for(int i=0;i<99;i++) {
                //这里的意思是每三条的orderId是一样的
                if(i%3==0){
                    orderId++;
                }

                String text = "the orderId is order"+orderId;
                //每三条的后缀分别是create，pay，finish
                if(i%3==0){
                    text+="-create";
                }else if(i%3==1){
                    text+="-pay";
                }else if(i%3==2){
                    text+="-finish";
                }
                //4.最基本的生产模式 topic+文本信息
                Message msg = new Message("topic_hello", "Tag-"+i, text.getBytes(RemotingHelper.DEFAULT_CHARSET));
                //5.获取发送响应
                SendResult sendResult = defaultMQProducer.send(msg, new MessageQueueSelector() {

                    // 选择发送消息的队列
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {

                        // arg的值其实就是orderId
                        Integer id = (Integer) arg;

                        // mqs是队列集合，也就是topic所对应的所有队列
                        int index = id % mqs.size();

                        // 这里根据前面的id对队列集合大小求余来返回所对应的队列
                        return mqs.get(index);
                    }
                }, orderId);



                System.out.println("发送结果为:" + JSON.toJSONString(sendResult));
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            //6.释放生产者
            defaultMQProducer.shutdown();
            System.out.println("生产者释放了");
        }


    }
}
