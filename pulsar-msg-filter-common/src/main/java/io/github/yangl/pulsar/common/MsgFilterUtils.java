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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.function.Supplier;

import static io.github.yangl.pulsar.common.MsgFilterConstants.AV_EVALUATOR;
import static io.github.yangl.pulsar.common.MsgFilterConstants.MSGMETADATA_PROPERTIES_NULL_REJECT;

@Slf4j
public class MsgFilterUtils {
    
    private static final Gson GSON = new GsonBuilder().create();
    
    // filter the message by expression
    public static boolean filter(String expression, Supplier<Map<String, Object>> supplier) {
        if (StringUtils.isBlank(expression) || StringUtils.equals(Boolean.TRUE.toString(), expression)) {
            return true;
        }
        
        Map<String, Object> messageProperties = supplier.get();
        if (messageProperties.isEmpty()) {
            if (MSGMETADATA_PROPERTIES_NULL_REJECT) {
                return false;
            } else {
                return true;
            }
        }
        
        Object rs = null;
        try {
            rs = AV_EVALUATOR.execute(expression, messageProperties);
        } catch (Exception e) {
            log.error(
                    "message filter expression execute by aviator error, the expression is: {}, messageProperties is: {}",
                    expression,
                    GSON.toJson(messageProperties),
                    e);
        }
        
        if (Boolean.FALSE.equals(rs)) {
            return false;
        }
        
        return true;
    }
    
    // clear the expression cache
    public static void clearExpressionCache() {
        AV_EVALUATOR.clearExpressionCache();
    }
    
}
