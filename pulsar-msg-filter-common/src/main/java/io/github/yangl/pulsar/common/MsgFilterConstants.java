package io.github.yangl.pulsar.common;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Feature;
import com.googlecode.aviator.Options;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class MsgFilterConstants {

    public static final String MSG_FILTER_EXPRESSION_KEY = "pulsar-msg-filter-expression";

    private static final String MSGMETADATA_PROPERTIES_NULL_REJECT_KEY = "msgmetadata-properties-null-reject";

    public static final boolean MSGMETADATA_PROPERTIES_NULL_REJECT;

    static {
        String nullReject = System.getProperty(
                MSGMETADATA_PROPERTIES_NULL_REJECT_KEY, System.getenv(MSGMETADATA_PROPERTIES_NULL_REJECT_KEY));
        if (StringUtils.isNotBlank(nullReject)) {
            MSGMETADATA_PROPERTIES_NULL_REJECT = Boolean.parseBoolean(nullReject);
        } else {
            MSGMETADATA_PROPERTIES_NULL_REJECT = true;
        }
    }

    public static final AviatorEvaluatorInstance AV_EVALUATOR;

    static {
        AV_EVALUATOR = AviatorEvaluator.getInstance();
        // only enable `If` `Return` feature
        Set<Feature> features = Feature.asSet(Feature.If, Feature.Return);
        AV_EVALUATOR.setOption(Options.FEATURE_SET, features);
    }
}
