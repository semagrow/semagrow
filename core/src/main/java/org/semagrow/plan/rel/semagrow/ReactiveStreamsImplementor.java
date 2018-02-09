package org.semagrow.plan.rel.semagrow;

import org.apache.calcite.plan.RelImplementor;
import org.reactivestreams.Publisher;

public class ReactiveStreamsImplementor implements RelImplementor {

    public Publisher<Object[]> implementRoot(ReactiveStreamsRel rel) {
        Bindable<Object[]> result = rel.implement(this);
        return result.bind(Bindable.DataContext.empty());
    }

}
