package org.semagrow.plan.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.List;

/**
 * Created by angel on 13/7/2017.
 */
public abstract class Service extends SingleRel {

    private RexNode ref;

    private boolean silent = false;

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input   Input relational expression
     */
    protected Service(RelOptCluster cluster,
                      RelTraitSet traits,
                      RelNode input,
                      RexNode ref,
                      boolean silent)
    {
        super(cluster, traits, input);

        this.ref = Preconditions.checkNotNull(ref);
        this.silent = silent;
    }

    public Service(RelInput input) {
        this(input.getCluster(),
            input.getTraitSet(),
            input.getInput(),
            input.getExpression("ref"),
            input.getBoolean("silent", false));
    }

    public RexNode getRef() { return ref; }

    @Override public List<RexNode> getChildExps() { return ImmutableList.of(ref); }

    @Override public final RelNode copy(RelTraitSet traitSet,
                                        List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), getRef(), isSilent());
    }

    public abstract Service copy(RelTraitSet traitSet, RelNode input,
                                RexNode ref, boolean silent);

    @Override public RelNode accept(RexShuttle shuttle) {
        RexNode ref = shuttle.apply(this.ref);
        if (this.ref == ref) {
            return this;
        }
        return copy(traitSet, getInput(), ref, isSilent());
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("ref", getRef())
                .itemIf("silent", isSilent(), isSilent());
    }

    public boolean isSilent() { return silent; }

}
