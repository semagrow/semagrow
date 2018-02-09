package org.semagrow.plan.rel.sparql;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;

/**
 * Created by angel on 13/7/2017.
 */
public class SparqlConvention extends Convention.Impl {

    private String endpoint;

    private SparqlDialect dialect;

    public SparqlConvention(SparqlDialect dialect, String endpoint) {
        super(dialect.toString() + "://" + endpoint, SparqlRel.class);

        this.endpoint = Preconditions.checkNotNull(endpoint);
        this.dialect = Preconditions.checkNotNull(dialect);
    }

    public SparqlDialect getDialect() { return dialect; }

    public String getEndpoint() { return endpoint; }

    public static SparqlConvention of(SparqlDialect dialect, String endpoint) {
        return new SparqlConvention(dialect, endpoint);
    }

    @Override public boolean satisfies(RelTrait trait) {
        if (this == trait)
            return true;
        else if (trait instanceof SparqlConvention) {
            SparqlConvention that = (SparqlConvention) trait;
            return this.getEndpoint().equals(that.getEndpoint()) &&
                   this.getDialect().isCompatible(that.getDialect());
        }
        return false;
    }

    @Override public void register(RelOptPlanner planner) {

        for (RelOptRule rule : SparqlRules.rules(this))
            planner.addRule(rule);

    }
}
