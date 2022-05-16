# pulsar-msg-filter-plugin
Msg filter plugin for [Apache Pulsar](https://github.com/apache/pulsar) on broker side.


----------------------------------------

`pulsar-msg-filter-plugin` 是一个基于`PIP 105: Support pluggable entry filter in Dispatcher` 为 [Apache Pulsar](https://github.com/apache/pulsar) 实现的 **服务端** 消息过滤插件。

### 特性介绍

1. 小巧、高性能
2. 支持常见条件表达式，满足各种业务过滤场景
3. ...

### 使用说明

1. 下载[pulsar-msg-filter-plugin-VERSION.nar](https://github.com/yangl/pulsar-msg-filter-plugin/releases/)插件并保存至指定目录，如/app/conf/plugin

2. 修改pulsar broker.conf配置（version >= 2.10），插件名称`pulsar-msg-filter`

   ```yaml
   # Class name of Pluggable entry filter that can decide whether the entry needs to be filtered
   # You can use this class to decide which entries can be sent to consumers.
   # Multiple classes need to be separated by commas.
   entryFilterNames=pulsar-msg-filter
   
   # The directory for all the entry filter implementations
   entryFiltersDirectory=/app/conf/plugin
   ```

3. 重启broker，查看日志，如果看到如下日志：

   `Successfully loaded entry filter for name` \`pulsar-msg-filter\`

   则说明配置成功

4. 验证（option）

   1. **发送方**构建Producer实例时关闭 `batch` 操作 **.enableBatching(false)**
   
      ```java
      Producer<String> producer = client.newProducer(Schema.STRING)
          .topic("test-topic-1")
          .enableBatching(false)
          .create();
      
      producer.newMessage()
          .property("k1","7")
          .property("k2", "vvvv")
          .property("k3", "true")
          .value("hi, this msg from `pulsar-msg-filter-plugin`")
          .send();
      ```

   2. **消费方**创建Consumer时添加 **pulsar-msg-filter-expression** 订阅属性:
   
      ```java
      Map<String, String> subscriptionProperties = Maps.newHashMap();
      subscriptionProperties.put("pulsar-msg-filter-expression", "long(k1)%10==7 || (k2=='vvvv' && k3=='false')");
      
      Consumer consumer = client.newConsumer()
          .topic("test-topic-1")
          .subscriptionName("my-subscription-1")
          .subscriptionProperties(subscriptionProperties)
          .subscribe();
      ```
   

**注意:**

1. 消息过滤过滤依赖`MessageMetadata`，故需关闭发送端的batch操作（默认开启）
2. SubscriptionProperties中的订阅**过滤条件key**固定为**`pulsar-msg-filter-expression`**
3. 由于pulsar message header的key&value全部为`String`类型，在使用表达式的时候注意将其类型转换至目标类型
4. AviatorScript的`false`判断个人建议直接使用字符串的 `==`  `true/false`比较，AviatorScript只有`nil false`为false，其他全部为true
5. `&& ||` 支持短路
6. 过滤引擎使用[AviatorScript](https://github.com/killme2008/aviatorscript) (感谢晓丹)，其内置函数详见其 [函数库列表](https://www.yuque.com/boyan-avfmj/aviatorscript/ashevw)

### License

`pulsar-msg-filter-plugin` is licensed under the [GPLv3 License](./LICENSE).

### Links

- https://pulsar.apache.org/

- https://github.com/killme2008/aviatorscript

  赞助我一杯美式 ^_^

  <img src="./weixin.png" width="30%" />
