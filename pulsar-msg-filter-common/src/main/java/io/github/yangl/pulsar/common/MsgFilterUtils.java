package io.github.yangl.pulsar.common;

import static io.github.yangl.pulsar.common.MsgFilterConstants.*;

import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class MsgFilterUtils {

    public static final boolean filter(String expression, Supplier<Map<String, Object>> supplier) {
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

        if (rs != null && Boolean.FALSE.equals(rs)) {
            return false;
        }

        return true;
    }
}
