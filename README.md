# pulsar-msg-filter-plugin
Msg filter plugin for [Apache Pulsar](https://github.com/apache/pulsar) on broker side.


----------------------------------------

`pulsar-msg-filter-plugin` 是一个基于`PIP 105: Support pluggable entry filter in Dispatcher` 为 [Apache Pulsar](https://github.com/apache/pulsar) 实现的 **服务端** 消息过滤插件。

### 特性介绍

1. 小巧、高性能
2. 支持各种复杂过滤场景
3. ...

### 使用

1. pulsar broker安装插件并配置（version >= 2.10），插件名称`name: pulsar-msg-filter-plugin`

2. 消费组订阅时添加`pulsar-msg-filter-expression`订阅属性，eg: 

   订阅消息头中 k1小于6 或者 k2是"vvvv" 且k3是false的消息

   ```java
   subscription.getSubscriptionProperties().put("pulsar-msg-filter-expression", "double(k1)<6 || (k2=='vvvv' && k3=='false')")
   ```

**注意:**

1. property的key固定为`pulsar-msg-filter-expression`
2. 由于pulsar message header的key&value全部为`String`类型，在使用表达式的时候注意其类型转换至目标类型
3. AviatorScript的`false`判断个人建议直接使用字符串的 `==`  `true/false`比较，AviatorScript只有`nil false`为false，其他全部为true
4. `&& ||` 支持短路
5. 过滤引擎使用[AviatorScript](https://github.com/killme2008/aviatorscript) (感谢晓丹)，其内置函数详见 [AV函数库列表](https://www.yuque.com/boyan-avfmj/aviatorscript/ashevw)

### Links

- https://pulsar.apache.org/

- https://github.com/killme2008/aviatorscript

  赞助我一杯美式 ^_^

  <img src="./weixin.png" width="30%" />
