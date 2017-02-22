package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Union;
import org.semagrow.plan.CompilerContext;
import org.semagrow.plan.Plan;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author acharal
 */
public class MinusBlock extends AbstractQueryBlock {

    private QueryBlock left;

    private QueryBlock right;

    public MinusBlock(QueryBlock left, QueryBlock right) {
        assert left != null && right != null;
        this.left = left;
        this.right = right;
    }

    public QueryBlock getLeft() { return left; }

    public QueryBlock getRight() { return right; }

    @Override
    public Set<String> getOutputVariables() { return left.getOutputVariables(); }

    @Override
    public <X extends Exception> void visitChildren(QueryBlockVisitor<X> visitor) throws X {
        for (QueryBlock b : new QueryBlock[]{left, right})
            b.visit(visitor);
    }

    public boolean hasDuplicates() { return false; }

    public Collection<Plan> getPlans(CompilerContext context) {
        Collection<Plan> leftPlans = left.getPlans(context);
        Collection<Plan> rightPlans = right.getPlans(context);
        //FIXME: prune if necessary
        return getMinusPlan(context, leftPlans, rightPlans);
    }

    public Collection<Plan> getMinusPlan(CompilerContext context, Collection<Plan> p1, Collection<Plan> p2) {
        //FIXME: Check the Site property
        return p1.stream().flatMap( pp1 ->
                p2.stream().flatMap(pp2 ->
                        Stream.of(context.asPlan(new Difference(pp1, pp2)))
                )
        ).collect(Collectors.toList());
    }
}
