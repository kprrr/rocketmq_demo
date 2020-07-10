package com.rocketmq.demo.listener;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

public class TransactionListenerImpl implements TransactionListener {

    //存储对应事务的状态信息 key：事务ID，value：当前事务执行的状态
    private ConcurrentHashMap<String,Integer> localTrans = new ConcurrentHashMap<>();
    /**
     * 执行本地事务
     * @param message
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        //事务ID
        String transactionId = message.getTransactionId();

        //0：执行中，状态未知，1：本地事务执行成功，2：本地事务执行失败
        localTrans.put(transactionId,0);

        //业务执行，处理本地事务，service
        System.out.println("hello!-----Demo-----Transaction");


        try {
            System.out.println("正在执行本地事务--------");
            Thread.sleep(120000);
            System.out.println("正在执行本地事务---------成功");
            localTrans.put(transactionId,1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            localTrans.put(transactionId,2);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }


        return LocalTransactionState.COMMIT_MESSAGE;
    }

    /**
     * 消息回查
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        //获取对应事务ID的状态
        String transactionId = messageExt.getTransactionId();
        //获取对应事务ID的执行状态
        Integer staus = localTrans.get(transactionId);

        System.out.println("消息回查-------transactionId:"+transactionId+",状态："+staus);
        switch (staus){
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.UNKNOW;
    }
}
