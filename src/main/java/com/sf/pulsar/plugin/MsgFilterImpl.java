package com.sf.pulsar.plugin;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;

import java.util.Map;

import static com.sf.pulsar.common.MsgFilterConstants.*;

@Slf4j
public class MsgFilterImpl implements EntryFilter {

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
			}
			else {
				return FilterResult.ACCEPT;
			}
		}

		Object rs = null;
		try {
			rs = AV_EVALUATOR.execute(expression, env);
		}
		catch (Exception e) {
			log.error("--aviator expression execute error, the expression is: {}, env is: {}", expression,
					GSON.toJson(env), e);
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
