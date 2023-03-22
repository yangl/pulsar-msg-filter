package com.sf.pulsar.server;

import static com.sf.pulsar.common.MsgFilterConstants.AV_EVALUATOR;
import static com.sf.pulsar.common.MsgFilterConstants.MSG_FILTER_EXPRESSION_KEY;

import com.google.common.collect.Maps;
import com.sf.pulsar.common.MsgFilterUtils;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;

@Slf4j
public class MsgFilterImpl implements EntryFilter {

    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {
        FilterResult rs = FilterResult.ACCEPT;
        // consumer subscription expression property
        String expression = null;
        Subscription subscription = context.getSubscription();
        if (subscription instanceof PersistentSubscription) {
            Map<String, String> subs = subscription.getSubscriptionProperties();
            expression = subs.get(MSG_FILTER_EXPRESSION_KEY);
        }

        Supplier<Map<String, Object>> msgMetadataSupplier = () -> {
            Map<String, Object> env = Maps.newHashMap();
            context.getMsgMetadata().getPropertiesList().forEach(kv -> env.put(kv.getKey(), kv.getValue()));
            return env;
        };

        boolean accept = MsgFilterUtils.filter(expression, msgMetadataSupplier);
        if (!accept) {
            rs = FilterResult.REJECT;
        }

        return rs;
    }

    @Override
    public void close() {
        AV_EVALUATOR.clearExpressionCache();
    }
}
