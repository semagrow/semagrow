package org.semagrow.plan.rel.semagrow;

import org.apache.calcite.plan.Convention;

public class ReactiveStreamsConvention extends Convention.Impl {

    public static ReactiveStreamsConvention INSTANCE = new ReactiveStreamsConvention();

    public ReactiveStreamsConvention() {
        super("REACTIVE", ReactiveStreamsRel.class);
    }
}
