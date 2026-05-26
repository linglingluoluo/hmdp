package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import jakarta.servlet.http.HttpSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2如果不符合,返回错误信息
            return Result.fail("手机号格式错误!");
        }

        //3符合,生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4.保存验证码到session
//        session.setAttribute("code", code);

        //4.保存验证码到redis, 设置有效期2分钟， set key val ex 120
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone,
                code, Duration.ofMinutes(RedisConstants.LOGIN_CODE_TTL));
        //5.发送验证码,因为要调用第三方api,所以这里不做实现
        log.info("发送短信验证码成功, 验证码 : {}", code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2如果不符合,返回错误信息
            return Result.fail("手机号格式错误!");
        }
        //2.从redis获取验证码并校验
        String redisCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        //听弹幕有问题,发送验证码后,如果又修改了手机号,会登陆成功
        if (redisCode == null || !redisCode.equals(code)) {
            //不一致, 报错
            return Result.fail("验证码错误");
        }
        //3. 一致,根据手机号查询用户
        //用mybatis-plus来查询用户
        User user = query().eq("phone", phone).one();
        //4.判断用户是否存在
        if (user == null) {
            //不存在,创建新用户并保存
            user = createUserWithPhone(phone);
        }
        //5.保存用户信息到redis
        //5.1随机生成token，作为登陆令牌
        String token = UUID.randomUUID().toString(true);
        //5.2 将user对象转为hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //Map<String, Object> userMap = BeanUtil.beanToMap(userDTO)会报错
        //ClassCastException,在将userMap往redis写时出现问题, long -> string错误,应该改为下面的
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)//忽略null值
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));//允许修改字段value
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        //5.3 设置token有效期, token有效期不应该固定，而应该随着用户登陆而刷新，可以在拦截器里设置
        stringRedisTemplate.expire(tokenKey, Duration.ofSeconds(RedisConstants.LOGIN_USER_TTL));
        //6.返回token
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        //保存用户
        //用mybatis-plus保存用户
        save(user);
        return user;
    }
}
