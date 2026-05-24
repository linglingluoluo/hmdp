package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import jakarta.servlet.http.HttpSession;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

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

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2如果不符合,返回错误信息
            return Result.fail("手机号格式错误!");
        }

        //3符合,生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4保存验证码到session
        session.setAttribute("code", code);
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
        //2.校验验证码
        String sessionCode = session.getAttribute("code").toString();
        String code = loginForm.getCode();
        //听弹幕有问题,发送验证码后,如果又修改了手机号,可能会登陆成功
        if(sessionCode == null || !sessionCode.equals(code)){
            //不一致, 报错
            return Result.fail("验证码错误");
        }
        //3. 一致,根据手机号查询用户
        //用mybatis-plus来查询用户
        User user = query().eq("phone", phone).one();
        //4.判断用户是否存在
        if(user == null){
            //不存在,创建新用户并保存
            user = createUserWithPhone(phone);
        }
        //5.保存用户信息到session
        session.setAttribute("user", user);
        return Result.ok();
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
