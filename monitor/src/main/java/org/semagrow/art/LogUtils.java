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

    public static boolean hasKobeQueryDesc(String query) {
        String keyword = "#kobeQueryDesc ";
        return query.startsWith(keyword);
    }

    public static String getKobeQueryDesc(String query) {
        String keyword = "#kobeQueryDesc ";
        assert hasKobeQueryDesc(query);
        int i = keyword.length();
        int j = query.indexOf('\n');
        return query.substring(i,j);
    }

    public static void initKobeReport() {
        MDC.put("kobeReport", "");
    }

    public static String getKobeReport() {
        return MDC.get("kobeReport");
    }

    public static void appendKobeReport(String s) {
        MDC.put("kobeReport", getKobeReport() + ", " + s);
    }
}
