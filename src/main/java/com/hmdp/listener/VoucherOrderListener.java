package com.hmdp.listener;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.rabbitmq.client.Channel;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import com.hmdp.utils.MQConstants;
import java.io.IOException;

@Component
@RequiredArgsConstructor
@Slf4j
public class VoucherOrderListener {
    @Resource
    private IVoucherOrderService voucherOrderService;
//    @RabbitListener(queues = {MQConstants.VOUCHER_QUEUE_NAME})
//    public void handleSimpleMessage(String message){
//        System.out.println("接收到消息: "+ message);
//    }
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = MQConstants.VOUCHER_QUEUE_NAME),
            key = MQConstants.VOUCHER_ROUTING_KEY,
            exchange = @Exchange(name = MQConstants.VOUCHER_EXCHANGE_NAME, type = ExchangeTypes.DIRECT)
    ))
/*    @RabbitListener(queues = {MQConstants.VOUCHER_QUEUE_NAME})*/
    public void handleVoucherMessage(Message message, Channel channel, VoucherOrder voucherOrder) {
        voucherOrderService.createVoucherOrder(voucherOrder);
        //这里已经在properties.yaml文件中设置了自动确认,不需要手动确认
/*        try {
            voucherOrderService.createVoucherOrder(voucherOrder);
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
        log.info("秒杀处理成功");
    }
}