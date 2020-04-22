package org.semagrow.art;

import org.slf4j.MDC;

import java.util.UUID;

public final class LogUtils {

    public static void setMDCifNull() {
        if (MDC.get("uuid") == null) {
            setMDC();
        }
    }

    public static void setMDC() {
        MDC.put("nestingLevel", "1");
        MDC.put("uuid", UUID.randomUUID().toString());
    }
}
