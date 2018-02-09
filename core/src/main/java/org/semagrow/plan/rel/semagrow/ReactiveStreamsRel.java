package org.semagrow.plan.rel.semagrow;

import org.apache.calcite.rel.RelNode;


public interface ReactiveStreamsRel extends RelNode {

    Bindable<Object[]> implement(ReactiveStreamsImplementor implementor);

}
