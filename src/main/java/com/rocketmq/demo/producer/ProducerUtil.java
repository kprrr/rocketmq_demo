package com.rocketmq.demo.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 生产者工具 此生产者只是包含了四种常用发送方法，其他自己加 */
public class ProducerUtil {
  private static final Logger logger = LoggerFactory.getLogger(ProducerUtil.class);
  // 默认生产者
  private DefaultMQProducer defaultMQProducer;
  // namesrv 命名服务地址
  private String namesrvAddr;
  // 分组名称
  private String producerGroupName;
  // 实例名称
  private String instanceName;

  /**
   * 初始化 生产者相关参数
   *
   * @throws MQClientException
   */
  public void init() throws MQClientException {
    // 参数信息
    // logger.info("DefaultMQProducer initialize!");
    logger.info(
        "producerGroupName="
            + producerGroupName
            + " &namesrvAddr="
            + namesrvAddr
            + " &instanceName="
            + instanceName);
      // 初始化 设置相关参数
      defaultMQProducer = new DefaultMQProducer(producerGroupName);
      defaultMQProducer.setNamesrvAddr(namesrvAddr);
      defaultMQProducer.setInstanceName(instanceName);
      defaultMQProducer.setRetryTimesWhenSendAsyncFailed(10);
      defaultMQProducer.setRetryTimesWhenSendFailed(10);
      defaultMQProducer.setRetryAnotherBrokerWhenNotStoreOK(true);
      defaultMQProducer.setSendMsgTimeout(5000);
      defaultMQProducer.start();
      logger.info("[DefaultMQProudcer 生产者初始化成功!]");
  }

    /**
     * 关闭生产者
     */
  public  void destroy() {
      defaultMQProducer.shutdown();
      logger.info("DefaultMQProudcer has stop success!");
  }
}
