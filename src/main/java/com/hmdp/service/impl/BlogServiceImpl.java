package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.Resource;
import jodd.util.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;

    @Override
    public Result queryBlogById(Long id) {
        //1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("博客不存在!");
        }
        //2.查询blog对应用户
        queryBlogUser(blog);
        //3.查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }


    @Override
    public Result queryHotBolg(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }
    //查询blog是否被当前登陆用户点过赞
    private void isBlogLiked(Blog blog) {
        //1.获取当前登陆用户
        UserDTO user = UserHolder.getUser();
        if(user == null){
            //用户未登陆,无需查询用户是否点赞
            return;
        }
        Long userId = user.getId();
        //2.判断当前用户是否已经点赞
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }
    //根据blog的userId, 获得icon和name并赋值给blog的对应field
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    //当前用户给id属性为id的blog点赞
    @Override
    public Result likeBlog(Long id) {
        //1.获取当前登陆用户
        Long userId = UserHolder.getUser().getId();
        //2.判断当前用户是否已经点赞
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        //用socre判断是否存在
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        //3.如果未点赞,可以点赞
        if (score == null) {
            //3.1 数据库点赞数  + 1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            //3.2.保存用户到redis的set集合里
            if (isSuccess) {
                //往zset中添加,时间戳作为分数, 表示先后顺序,
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            //4 已点赞,取消点赞
            //4.1 数据库点赞数 + 1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //4.2 把用户从redis的set集合里移除
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }
    //查询blog点赞时间top5的用户
    @Override
    public Result queryBlogLikes(Long id) {
        //1. 查询点赞top5的用户
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(RedisConstants.BLOG_LIKED_KEY + id, 0, 4);
        if(top5 == null || top5.isEmpty()){
            return Result.ok(Collections.EMPTY_LIST);
        }
        //2.解析其中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).toList();
        //3.根据用户id查询
        //问题 : 下面的sql查询语句
        //SELECT id,phone,password,nick_name,icon,create_time,update_time FROM tb_user WHERE id IN (5 , 1 )
        //不保证返回顺序还是按照5,1, 所以会出现查询出来的点赞top5显示不是按照查询的顺序显示的
//        List<User> users = userService.listByIds(ids);
//        List<UserDTO> userDTOs = users.stream().map(user ->
//                BeanUtil.copyProperties(user, UserDTO.class)
//        ).toList();
        //解决方法:使用WHERE id IN (5 , 1 ) ORDER BY FIELD(id, 5, 1)
        String idStr = StrUtil.join(",", ids);
        List<UserDTO> userDTOs = userService
                .query()
                .in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .toList();
        return Result.ok(userDTOs);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2.保存探店博文
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("创建笔记失败");
        }
        //3.查询笔记作者的所有粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //4.推送blog给所有follow
        for (Follow follow : follows) {
            //4.1 获取粉丝id
            Long userId = follow.getUserId();
            //4.2 推送
            String key = "feed:" + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());

    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //1.获取用户的收件箱
        Long userId = UserHolder.getUser().getId();
        String key = RedisConstants.FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.
                opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 3);
        //非空判断
        if(CollectionUtil.isEmpty(typedTuples)){
            return Result.ok();
        }
        //2. 解析数据, blogId, minTime(时间戳), offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        //下一次的查询的最大时间戳和偏移量
        //时间由大到小排列, 将这次查询获得的最小时间戳作为下次查询的最大时间戳
        long newTime = 0;
        int newOffset = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            //2.1 获取id
            ids.add(Long.valueOf(typedTuple.getValue()));
            //2.2 获取时间戳
            long time = typedTuple.getScore().longValue();
            if(time == newTime){
                newOffset++;
            } else {
                newTime = time;
                newOffset = 1;
            }
        }
        //3.根据id查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Blog blog : blogs) {
            //3.1查询blog对应用户
            queryBlogUser(blog);
            //3.2 查询blog是否被点赞
            isBlogLiked(blog);
        }
        //4. 封装结果并返回
        ScrollResult res = new ScrollResult();
        res.setList(blogs);
        res.setOffset(newOffset);
        res.setMinTime(newTime);
        return Result.ok(res);
    }

}
