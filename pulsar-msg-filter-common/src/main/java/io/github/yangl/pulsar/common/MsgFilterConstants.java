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
package io.github.yangl.pulsar.common;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Feature;
import com.googlecode.aviator.Options;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

public class MsgFilterConstants {
    
    public static final String MSG_FILTER_EXPRESSION_KEY = "pulsar-msg-filter-expression";
    
    private static final String MSGMETADATA_PROPERTIES_NULL_REJECT_KEY = "msgmetadata-properties-null-reject";
    
    public static final boolean MSGMETADATA_PROPERTIES_NULL_REJECT;
    
    static {
        String rejVal = System.getProperty(
                MSGMETADATA_PROPERTIES_NULL_REJECT_KEY, System.getenv(MSGMETADATA_PROPERTIES_NULL_REJECT_KEY));
        if (StringUtils.isNotBlank(rejVal)) {
            MSGMETADATA_PROPERTIES_NULL_REJECT = Boolean.parseBoolean(rejVal);
        } else {
            MSGMETADATA_PROPERTIES_NULL_REJECT = false;
        }
    }
    
    static final AviatorEvaluatorInstance AV_EVALUATOR;
    
    static {
        AV_EVALUATOR = AviatorEvaluator.getInstance();
        AV_EVALUATOR.setCachedExpressionByDefault(true);
        AV_EVALUATOR.useLRUExpressionCache(200);
        
        // only enable `If` `Return` feature
        Set<Feature> features = Feature.asSet(Feature.If, Feature.Return);
        AV_EVALUATOR.setOption(Options.FEATURE_SET, features);
    }
}
