# pulsar-msg-filter
message filter for [Apache Pulsar](https://github.com/apache/pulsar), both support server-side and client-side.


----------------------------------------

`pulsar-msg-filter-plugin` 是一个基于`PIP 105: Support pluggable entry filter in Dispatcher` 为 [Apache Pulsar](https://github.com/apache/pulsar) 实现的 **服务端** 消息过滤插件。

`pulsar-msg-filter-interceptor` 是一个基于 Pulsar `ConsumerInterceptor` 实现的 **客户端** 消息过滤拦截器。

### 特性介绍

1. 高性能、小巧
2. 支持常见条件表达式，几乎满足各种业务过滤场景

<details><summary><b>[server-side] pulsar-msg-filter-plugin 使用说明</b></summary>

1. 下载[pulsar-msg-filter-plugin-VERSION.nar](https://github.com/yangl/pulsar-msg-filter/releases/)插件并保存至指定目录，如/app/conf/plugin

2. 修改pulsar broker.conf配置（version >= 2.10），插件名称`pulsar-msg-filter`

   ```yml
   # Class name of Pluggable entry filter that can decide whether the entry needs to be filtered
   # You can use this class to decide which entries can be sent to consumers.
   # Multiple classes need to be separated by commas.
   entryFilterNames=pulsar-msg-filter
   
   # The directory for all the entry filter implementations
   entryFiltersDirectory=/app/plugin
   # Location of unpacked NAR file
   narExtractionDirectory=/app/nar
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

    2. **消费方**使用admin配置订阅消费组过滤表达式，其key固定为 **pulsar-msg-filter-expression**

         ##### 注：复杂表达式记得添加 "" 防止被转义

        ```shell
        pulsar-admin topics update-subscription-properties --property --property pulsar-msg-filter-expression="double(k1)<6 || (k2=='vvvv' && k3=='true')" --subscription 订阅组名称 主题
        
        pulsar-admin topics get-subscription-properties --subscription 订阅组名称 主题
        ```
        
        ##### 如上配置修改后立马生效，无需在创建Consumer时再设置subscriptionProperties 
        
         ```java

           Consumer consumer = client.newConsumer()
             .subscriptionName("订阅组名称")
             .topic("主题")
             .subscribe();
         ```
           
        ##### 说明: pulsar-msg-filter-plugin插件（服务端）依赖消息的`MessageMetadata`，故需**关闭发送端的batch**操作，否则无效（`.enableBatching(false)`），如无法关闭可配合pulsar-msg-filter-interceptor 一起使用。
</details>


<details><summary><b>[client-side] pulsar-msg-filter-interceptor 使用说明</b></summary>

1. 添加 pulsar-msg-filter-interceptor 依赖
   ```xml
    <dependency>
        <groupId>io.github.yangl</groupId>
        <artifactId>pulsar-msg-filter-interceptor</artifactId>
        <version>VERSION</version>
    </dependency>
   ```

2. 创建Consumer实例时配置 **MsgFilterConsumerInterceptor** 过滤器
    ```java
    Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .subscriptionName("订阅组名称")
            .topic("主题")
            .intercept(MsgFilterConsumerInterceptor.<String>builder().build())
            .subscribe();
    ```
    ##### 说明: 如果创建client时使用的是 `pulsar://` 开头的地址，需额外使用`http://`设置 `.webServiceUrl(YOUR_HTTP_SERVICE_URL)` 参数。
    ```java
    .intercept(MsgFilterConsumerInterceptor.<String>builder().webServiceUrl(YOUR_HTTP_SERVICE_URL).build())
    ```

</details>

<details><summary><b>注意事项</b></summary>

 - 由于pulsar message header的key&value全部为`String`类型，在使用表达式的时候注意将其类型转换至目标类型
 - AviatorScript的`false`判断个人建议直接使用字符串的 `==`  `true/false`比较，AviatorScript只有`nil false`为false，其他全部为true
 - 过滤引擎使用[AviatorScript](https://github.com/killme2008/aviatorscript) (感谢晓丹)，其内置函数详见其 [函数库列表](https://www.yuque.com/boyan-avfmj/aviatorscript/ashevw)

</details>

### License

`pulsar-msg-filter` is licensed under the [AGPLv3 License](./LICENSE).

### Links

- https://pulsar.apache.org/

- https://github.com/killme2008/aviatorscript

  赞助我一杯美式 ^_^

  <img src="./weixin.png" width="30%" />
