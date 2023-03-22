package io.github.yangl.pulsar.common;

import static io.github.yangl.pulsar.common.MsgFilterConstants.*;

import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class MsgFilterUtils {

    public static final boolean filter(String expression, Supplier<Map<String, Object>> supplier) {
        if (StringUtils.isBlank(expression)) {
            return true;
        }

        Map<String, Object> env = supplier.get();
        // expression is not null && env is null
        if (env.isEmpty()) {
            if (MSGMETADATA_PROPERTIES_NULL_REJECT) {
                return false;
            } else {
                return true;
            }
        }

        Object rs = null;
        try {
            rs = AV_EVALUATOR.execute(expression, env);
        } catch (Exception e) {
            log.error(
                    "--aviator expression execute error, the expression is: {}, env is: {}",
                    expression,
                    GSON.toJson(env),
                    e);
        }

        if (rs != null && Boolean.FALSE.equals(rs)) {
            return false;
        }

        return true;
    }
}
