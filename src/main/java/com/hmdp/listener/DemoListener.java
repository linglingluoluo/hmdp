package com.hmdp.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DemoListener {
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = "direct.demo.queue"),
            key = "direct.demo",
            exchange = @Exchange(name = "hmdp.demo", type = ExchangeTypes.DIRECT)
    ))
    public void listenWithRetry(String msg){
        System.out.println("listener has received msg :  " + msg);
//        throw new MessageConversionException("抛出了消息处理失败异常on purpose");
        throw new RuntimeException("抛出了消费者处理失败异常on purpose");
    }
}
