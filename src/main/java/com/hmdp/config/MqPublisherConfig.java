package com.hmdp.config;

import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.amqp.core.Message;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;

//配置RabbitMQ的publisher returnCallback和confirmCallback
@Slf4j
@AllArgsConstructor
//@Configuration  //不需要消息发送重试,先注释掉
public class MqPublisherConfig {

    private final RabbitTemplate rabbitTemplate;


    @PostConstruct
    public void init() {
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setReturnsCallback(returned -> {
            System.out.println("收到 Return 消息");

            Message message = returned.getMessage();
            String body = new String(message.getBody(), StandardCharsets.UTF_8);

            System.out.println("消息内容：" + body);
            System.out.println("replyCode：" + returned.getReplyCode());
            System.out.println("replyText：" + returned.getReplyText());
            System.out.println("exchange：" + returned.getExchange());
            System.out.println("routingKey：" + returned.getRoutingKey());
        });

        rabbitTemplate.setConfirmCallback(new RabbitTemplate.ConfirmCallback() {
            /**
             * 只要broker收到消息，就会触发这个回调
             * @param correlationData 当前消息的唯一关联数据（消息的唯一id）
             * @param ack 消息是否被broker接收到
             * @param s 失败的原因
             */
            @Override
            public void confirm(CorrelationData correlationData, boolean ack, String s) {
                System.out.println("confirm:    " + correlationData);
                System.out.println("is ack ? : " + ack + "  " + s);
            }
        });
    }
}
