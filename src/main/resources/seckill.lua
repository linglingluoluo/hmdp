-- 1.参数列表
-- 1.1 优惠券id
local voucherId = ARGV[1]
-- 1.2 用户id
local userId = ARGV[2]
-- 1.3 订单id
local orderId = ARGV[3]

-- 2.数据key
--2.1 库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2 订单key
local orderKey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- 3.1. 判断库存是否充足 getStockKey, get返回的是字符串，需要转化为数字才能进行判断
local stock = tonumber(redis.call('get', stockKey));
if(stock == nil or stock <= 0) then
	-- 3.2 库存不足，返回1
	return 1
end
-- 3.2 判断用户是否下单 SISMEMBER orderKye userId
if(redis.call('sismember',  orderKey, userId) == 1) then
	-- 3.3.存在， 说明是重复下单，返回 2
	return 2
end
-- 3.4 扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 3.5 下单（保存用户）sadd orderKey userId
redis.call('sadd', orderKey, userId)
-- 以下已弃用, 改为rabbitmq做消息队列
-- --3.6 发送消息到队列中 xadd stream.orders * k1 v1 k2 v2...
-- -- orderId的key为id而不是orderId, 为了方便后面存到Java对象中
-- redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId);
return 0




