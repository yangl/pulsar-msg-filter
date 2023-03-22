package com.sf.pulsar.interceptor;

import com.google.common.collect.Maps;
import com.sf.pulsar.common.MsgFilterConstants;
import com.sf.pulsar.common.MsgFilterUtils;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

@Slf4j
public class MsgFilterConsumerInterceptor<T> implements ConsumerInterceptor<T> {

    @Override
    public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {

        String exp = null;
        if (consumer instanceof ConsumerBase) {
            try {
                ClientConfigurationData client =
                        ((ConsumerBase<T>) consumer).getClient().getConfiguration();
                String url = client.getServiceUrl();

                PulsarAdmin admin = PulsarAdmin.builder()
                        .serviceHttpUrl(url)
                        .authentication(client.getAuthentication())
                        .build();
                Map<String, String> subscriptionProperties =
                        admin.topics().getSubscriptionProperties(message.getTopicName(), consumer.getSubscription());

                exp = subscriptionProperties.get(MsgFilterConstants.MSG_FILTER_EXPRESSION_KEY);

                // 反射获取到的只是客户端初始化时的配置，如果使用admin客户端修改更新后通过反射获取到就不是最新的了

                // Field confField = ConsumerBase.class.getDeclaredField("conf");
                // if (!confField.canAccess(consumer)) {
                // confField.setAccessible(true);
                // }
                // ConsumerConfigurationData conf =
                // (ConsumerConfigurationData)confField.get(consumer);
                // Map<String, String> subscriptionProperties =
                // conf.getSubscriptionProperties();

            } catch (PulsarClientException | PulsarAdminException e) {
                throw new RuntimeException(e);
            }
        }

        boolean accept = MsgFilterUtils.filter(exp, () -> {
            Map<String, Object> env = Maps.newHashMap();
            message.getProperties().forEach((k, v) -> env.put(k, v));
            return env;
        });

        if (!accept) {
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                log.error("client filter drop the message", e);
            } finally {
                return null;
            }
        }

        return message;
    }

    @Override
    public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {}

    @Override
    public void onAcknowledgeCumulative(Consumer<T> consumer, MessageId messageId, Throwable exception) {}

    @Override
    public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> messageIds) {}

    @Override
    public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> messageIds) {}

    @Override
    public void onPartitionsChange(String topicName, int partitions) {
        ConsumerInterceptor.super.onPartitionsChange(topicName, partitions);
    }

    @Override
    public void close() {}
}
