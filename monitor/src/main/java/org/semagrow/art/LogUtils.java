package org.semagrow.art;

import org.slf4j.MDC;

import java.util.UUID;

public final class LogUtils {

    public static String getNewQueryID() {
        MDC.put("qid", UUID.randomUUID().toString().substring(0,8));
        return MDC.get("qid");
    }

    public static String getQueryID() {
        return MDC.get("qid");
    }
}
