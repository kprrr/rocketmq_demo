package com.rocketmq.demo.producer;

import com.rocketmq.demo.listener.TransactionListenerImpl;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

@Component
public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //DefaultMQProducer producer = new DefaultMQProducer("demo_prodecer_group");
        TransactionMQProducer producer = new TransactionMQProducer("demo_transactionProducer_group");
        producer.setNamesrvAddr("192.168.40.129:9876");

        //指定消息监听对象，用于执行本地事务和消息回查
        TransactionListener transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);

        //线程池
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(
                        2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                       Thread thread = new Thread(r);
                       thread.setName("client-transaction-msg-check-thread");
                       return thread;
                    }
                }
        );
        producer.setExecutorService(executorService);

        producer.start();

        Message msg = new Message("Topic_Transaction_Demo",
                                    "tags",
                                    "keys_T",
                                    "hello!-Transaction".getBytes(RemotingHelper.DEFAULT_CHARSET));
        //发送事务消息





        SendResult result = producer.send(msg);
        System.out.println(result);
        producer.shutdown();


    }
}
