# pulsar-msg-filter
message filter for [Apache Pulsar](https://github.com/apache/pulsar), both support server-side and client-side.

----------------------------------------

[简体中文](README.md) | [English](README-en.md)


`pulsar-msg-filter-plugin` is a **server-side** message filtering plugin for [Apache Pulsar](https://github.com/apache/pulsar) based on `PIP 105: Support pluggable entry filter in Dispatcher`.

`pulsar-msg-filter-interceptor` is a **client-side** message filtering interceptor implemented based on Pulsar `ConsumerInterceptor`.

### Features

1. High performance and small size
2. Supports common conditional expressions, almost meeting various business filtering scenarios

<details><summary><b>[server-side] pulsar-msg-filter-plugin usage instructions</b></summary>

1. Download the [pulsar-msg-filter-plugin-VERSION.nar](https://github.com/yangl/pulsar-msg-filter/releases/) plugin and save it to the specified directory, such as /app/conf/plugin.

2. Modify the pulsar broker.conf configuration (version >= 2.10), with the plugin name set to `pulsar-msg-filter`:

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

3. Restart broker and check logs. If you see log messages like `Successfully loaded entry filter for name` \`pulsar-msg-filter\`, then it means that configuration was successful.

4. Verification (optional)

    1. When building **Producer** instance, **disable batch** operations using **.enableBatching(false)**.

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

    2. When consuming messages, configure subscription group filtering expression using `admin configuration`. The key is fixed as **pulsar-msg-filter-expression**.

         ##### Note: For complex expressions, remember to add "" to prevent them from being escaped.

        ```shell
        pulsar-admin topics update-subscription-properties --property --property pulsar-msg-filter-expression="double(k1)<6 || (k2=='vvvv' && k3=='true')" --subscription SUBSCRIPTIONNAME TOPIC
        
        pulsar-admin topics get-subscription-properties --subscription SUBSCRIPTIONNAME TOPIC
        ```
        
        ##### After modifying the above configuration, it takes effect immediately without needing to set subscriptionProperties when creating Consumer.
        
         ```java

           Consumer consumer = client.newConsumer()
             .subscriptionName("SUBSCRIPTIONNAME")
             .topic("TOPIC")
             .subscribe();
         ```
           
        ##### Explanation: Since the pulsar-msg-filter-plugin plugin (server-side) depends on `MessageMetadata` of messages, batch operations on the sending end need to be **disabled** (`.enableBatching(false)`), otherwise they will not work. If unable to disable batching, use it together with pulsar-msg-filter-interceptor.

</details>


<details><summary><b>[client-side] pulsar-msg-filter-interceptor usage instructions</b></summary>

1. Add dependency for pulsar-msg-filter-interceptor
   ```xml
    <dependency>
        <groupId>io.github.yangl</groupId>
        <artifactId>pulsar-msg-filter-interceptor</artifactId>
        <version>VERSION</version>
    </dependency>
   ```

2. When creating Consumer instance, configure **MsgFilterConsumerInterceptor** filter:
    ```java
    Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .subscriptionName("SUBSCRIPTIONNAME")
            .topic("TOPIC")
            .intercept(MsgFilterConsumerInterceptor.<String>builder().build())
            .subscribe();
    ```
    ##### Note: If you are using an address starting with "pulsar://", you need to additionally set the `.webServiceUrl(YOUR_HTTP_SERVICE_URL)` parameter as follows:
    ```java
    .intercept(MsgFilterConsumerInterceptor.<String>builder().webServiceUrl(YOUR_HTTP_SERVICE_URL).build())
    ```

</details>

<details><summary><b>Precautions</b></summary>

 - Since all keys and values in Pulsar message headers are of type `String`, pay attention to converting their types into target types when using expressions.
 - For `false` judgments in AviatorScript, it is recommended to directly use string comparison with `==`  `true/false`. In AviatorScript, only `nil` and `false` are considered false, while all others are considered true.
 - The filtering engine uses [AviatorScript](https://github.com/killme2008/aviatorscript) (thanks to Xiaodan), and its [built-in functions](https://www.yuque.com/boyan-avfmj/aviatorscript/ashevw) can be found in the function library list.

</details>

### License

`pulsar-msg-filter` is licensed under the [AGPLv3 License](./LICENSE).

### Links

- https://pulsar.apache.org/

- https://github.com/killme2008/aviatorscript

