package org.semagrow.plan.rel.sparql;


import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlOperator;

import java.util.Map;

public interface SparqlFeature {

    boolean isStandard();

    SparqlVersion getRequiredSparqlVersion();


    static Map<Object, SparqlFeature> features = Maps.newHashMap();

    static SparqlFeature of(SqlOperator op) {
        if (op instanceof SparqlFeature)
            return (SparqlFeature)op;
        else
            return features.get(op);
    }

    static <T> T sparql10(T op) {
        if (op != null)
            features.put(op, StandardSparqlFeature.STD_SPARQL10);
        return op;
    }

    static <T> T sparql11(T op) {
        if (op != null)
            features.put(op, StandardSparqlFeature.STD_SPARQL11);
        return op;
    }

    enum StandardSparqlFeature implements SparqlFeature {

        STD_SPARQL10(SparqlVersion.SPARQL10),
        STD_SPARQL11(SparqlVersion.SPARQL11);

        private SparqlVersion v;

        StandardSparqlFeature(SparqlVersion v) {
            this.v = v;
        }

        public SparqlVersion getRequiredSparqlVersion() { return v; }
        public boolean isStandard() { return true; }
    }

}
