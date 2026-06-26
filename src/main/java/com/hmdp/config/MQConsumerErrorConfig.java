package com.hmdp.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//定义消费者处理消息失败重试次数耗尽后, 将失败消息投递到别的队列
@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "spring.rabbitmq.listener.simple.retry", name = "enabled", havingValue = "true")  //仅在开启了消费者重试机制后的属性配置, 即ymal文件对于属性
public class MQConsumerErrorConfig {
    //定义消费者处理消息失败重试次数耗尽后, 将消息投递到指定队列,交换机,routing key
    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange("error.direct");
    }

    @Bean
    public Queue errorQueue() {
        return new Queue("direct.error.queue");
    }

    @Bean
    public Binding errorBind(Queue errorQueue, DirectExchange directExchange) {
        return BindingBuilder.bind(errorQueue).to(directExchange).with("error");
    }

    //定义消费者处理消息失败重试次数耗尽后, 将消息投递到指定队列所需MessageRecover
    @Bean
    public MessageRecoverer messageRecoverer(RabbitTemplate rabbitTemplate) {
        log.info("MessageRecoverer加载成功...");
        return new RepublishMessageRecoverer(rabbitTemplate, "error.direct", "error");
    }
}
