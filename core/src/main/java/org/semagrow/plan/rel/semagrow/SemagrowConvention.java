package org.semagrow.plan.rel.semagrow;

import org.apache.calcite.plan.Convention;

public class SemagrowConvention extends Convention.Impl {

    public static SemagrowConvention INSTANCE = new SemagrowConvention();

    private SemagrowConvention() {
        super("SEMAGROW", SemagrowRel.class);
    }
}
