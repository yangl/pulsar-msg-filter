package com.sf.pulsar.plugin;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Feature;
import com.googlecode.aviator.Options;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;

import java.util.Map;
import java.util.Set;


@Slf4j
public class MsgFilterImpl implements EntryFilter {

    private static final Gson GSON = new GsonBuilder().create();
    public static final String MSG_FILTER_EXPRESSION_KEY = "pulsar-msg-filter-expression";

    private static final AviatorEvaluatorInstance AV_EVALUATOR;

    private static final String MSGMETADATA_PROPERTIES_NULL_REJECT_KEY = "msgmetadata-properties-null-reject";
    private static boolean MSGMETADATA_PROPERTIES_NULL_REJECT = true;


    static {
        AV_EVALUATOR = AviatorEvaluator.getInstance();
        // only enable `If` `Return` feature
        Set<Feature> features = Feature.asSet(Feature.If, Feature.Return);
        AV_EVALUATOR.setOption(Options.FEATURE_SET, features);
        String nullReject = System.getProperty(MSGMETADATA_PROPERTIES_NULL_REJECT_KEY, System.getenv(MSGMETADATA_PROPERTIES_NULL_REJECT_KEY));
        if (StringUtils.isNotBlank(nullReject)) {
            MSGMETADATA_PROPERTIES_NULL_REJECT = Boolean.parseBoolean(nullReject);
        }
    }

    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {
        // consumer subscription expression property
        String expression = null;
        Subscription subscription = context.getSubscription();
        if (subscription instanceof PersistentSubscription) {
            Map<String, String> subs = ((PersistentSubscription) subscription).getSubscriptionProperties();
            expression = subs.get(MSG_FILTER_EXPRESSION_KEY);
        }

        if (StringUtils.isBlank(expression)) {
            return FilterResult.ACCEPT;
        }

        Map<String, Object> env = Maps.newHashMap();
        context.getMsgMetadata().getPropertiesList().forEach(kv -> env.put(kv.getKey(), kv.getValue()));

        // expression is not null && env is null
        if (env.isEmpty()) {
            if (MSGMETADATA_PROPERTIES_NULL_REJECT) {
                return FilterResult.REJECT;
            } else {
                return FilterResult.ACCEPT;
            }
        }

        Object rs = null;
        try {
            rs = AV_EVALUATOR.execute(expression, env);
        } catch (Exception e) {
            log.error("--aviator expression execute error, the expression is: {}, env is: {}", expression, GSON.toJson(env), e);
        }

        if (rs != null && Boolean.FALSE.equals(rs)) {
            return FilterResult.REJECT;
        }

        return FilterResult.ACCEPT;
    }

    @Override
    public void close() {
        AV_EVALUATOR.clearExpressionCache();
    }
}
