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
package io.github.yangl.pulsar.server.entryfilter;

import static io.github.yangl.pulsar.common.MsgFilterConstants.AV_EVALUATOR;
import static io.github.yangl.pulsar.common.MsgFilterConstants.MSG_FILTER_EXPRESSION_KEY;

import com.google.common.collect.Maps;
import io.github.yangl.pulsar.common.MsgFilterUtils;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;

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
