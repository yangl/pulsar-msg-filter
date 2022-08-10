package com.sf.pulsar.interceptor;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

import java.util.Set;

public class MsgFilterConsumerInterceptor<T> implements ConsumerInterceptor<T> {
    @Override
    public void close() {}

    @Override
    public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
        // filter msg logic
        // TODO
        // need consumer export the subscription properties feature.

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
}
