package com.hmdp.config;


import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
@Slf4j
public class RabbitMQConfig {
    //这段代码是示例: Spring AMQP 中用于声明 RabbitMQ 消费者的注解配置。它的作用是监听指定队列，并绑定到交换机。
/*    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "direct.seckill.queue"),
            key = "direct.seckill",
            exchange = @Exchange(name = "hmdp.direct", type = ExchangeTypes.DIRECT)
    ))
    public void handleSeckillMessage(String message) {
        log.info("接收到消息：{}", message);
    }*/

    // Spring AMQP 中配置消息转换器的 Bean 定义。它的作用是将 Java 对象自动序列化为 JSON 格式进行传输，并在接收时自动反序列化回 Java 对象。
    @Bean
    public MessageConverter messageConverter(){
        // 1.定义消息转换器
        Jackson2JsonMessageConverter jjmc = new Jackson2JsonMessageConverter();
        // 2.配置自动创建消息id，用于识别不同消息，也可以在业务中基于ID判断是否是重复消息
        jjmc.setCreateMessageIds(true);
        return jjmc;
    }
}