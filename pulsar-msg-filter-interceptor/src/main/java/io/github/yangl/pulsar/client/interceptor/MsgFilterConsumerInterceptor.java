/*
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.github.yangl.pulsar.client.interceptor;

import com.google.common.collect.Maps;
import io.github.yangl.pulsar.common.MsgFilterConstants;
import io.github.yangl.pulsar.common.MsgFilterUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class MsgFilterConsumerInterceptor<T> implements ConsumerInterceptor<T> {
    
    // cache for the expression, the key format is subName@__@topicName
    private static final ConcurrentMap<String, String> SUB_TOPIC_EXP = Maps.newConcurrentMap();
    
    @Override
    public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
        
        String subName = consumer.getSubscription();
        String topicName =
                TopicName.getPartitionedTopicName(message.getTopicName()).toString();
        
        String key = StringUtils.join(subName, "@__@", topicName);
        String expression = SUB_TOPIC_EXP.get(key);
        
        if (StringUtils.isBlank(expression)) {
            if (consumer instanceof ConsumerBase) {
                try {
                    ClientConfigurationData ccd =
                            ((ConsumerBase<T>) consumer).getClient().getConfiguration();
                    String url = ccd.getServiceUrl();
                    
                    PulsarAdmin admin = PulsarAdmin.builder()
                            .serviceHttpUrl(url)
                            .authentication(ccd.getAuthentication())
                            .build();
                    
                    Map<String, String> subp = admin.topics().getSubscriptionProperties(topicName, subName);
                    admin.close();
                    
                    expression = subp.get(MsgFilterConstants.MSG_FILTER_EXPRESSION_KEY);
                    
                    if (StringUtils.isBlank(expression)) {
                        expression = Boolean.TRUE.toString();
                    }
                    
                    SUB_TOPIC_EXP.put(key, expression);
                    
                    // What is obtained through reflection is only the configuration of the client during
                    // initialization. If it is updated or modified using the admin client, the configuration obtained
                    // through reflection will not be the latest.
                    
                    // Field confField = ConsumerBase.class.getDeclaredField("conf");
                    // if (!confField.canAccess(consumer)) {
                    // confField.setAccessible(true);
                    // }
                    // ConsumerConfigurationData conf =
                    // (ConsumerConfigurationData)confField.get(consumer);
                    // Map<String, String> subp =
                    // conf.getSubscriptionProperties();
                    
                } catch (PulsarClientException | PulsarAdminException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        
        boolean accept = MsgFilterUtils.filter(expression, () -> {
            Map<String, Object> env = Maps.newHashMap();
            message.getProperties().forEach((k, v) -> env.put(k, v));
            return env;
        });
        
        if (!accept) {
            try {
                consumer.acknowledge(message);
            } catch (PulsarClientException e) {
                log.error("consumer interceptor drop the message", e);
            } finally {
                // drop the message
                return null;
            }
        }
        
        return message;
    }
    
    @Override
    public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {
    }
    
    @Override
    public void onAcknowledgeCumulative(Consumer<T> consumer, MessageId messageId, Throwable exception) {
    }
    
    @Override
    public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> messageIds) {
    }
    
    @Override
    public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> messageIds) {
    }
    
    @Override
    public void onPartitionsChange(String topicName, int partitions) {
        ConsumerInterceptor.super.onPartitionsChange(topicName, partitions);
    }
    
    @Override
    public void close() {
    }
}
