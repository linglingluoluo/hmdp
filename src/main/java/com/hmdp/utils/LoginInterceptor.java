package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.time.Duration;
import java.util.Map;

@Component
public class LoginInterceptor implements HandlerInterceptor {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.获取请求头中的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            //不存在,拦截,返回401状态码
            response.setStatus(401);
            return false;
        }
        //2/基于token获取redis的用户
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().
                entries(tokenKey);
        //3.判断用户是否存在
        if(userMap.isEmpty()){
            //不存在,拦截,返回401状态码
            response.setStatus(401);
            return false;
        }
        //将查询到的hash数据转为对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        //4.存在,保存信息到ThreadLocal
        UserHolder.saveUser(userDTO);
        //5.刷新token有效期10h = 36000min
        stringRedisTemplate.expire(tokenKey, Duration.ofSeconds(RedisConstants.LOGIN_USER_TTL));
        return true; //放行
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser(); //避免内存泄露
    }
}
