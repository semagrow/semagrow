package org.semagrow.plan.querygraph;

/**
 * Created by angel on 18/5/2015.
 */
class LeftJoinPredicate extends QueryPredicate {

    public LeftJoinPredicate(String variable) { this.joinVariable = variable; }

    private String joinVariable;
}