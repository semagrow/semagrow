package org.semagrow.plan.queryblock;

/**
 * Modifies the DuplicateStrategy of the QueryBlock graph to lift non-essential
 * constraints for duplicate elimination and facilitate other optimizations.
 *
 * @see SelectMergeVisitor
 * @see ExistToEachQuantificationVisitor
 * @author acharal
 * @since 2.0
 */
public class DistinctStrategyVisitor extends AbstractQueryBlockVisitor<RuntimeException> {


    @Override
    public void meet(SelectBlock b) throws RuntimeException {

        for (Quantifier q : b.getQuantifiers()) {
            if (   q.getQuantification() == Quantifier.Quantification.ANY
                || q.getQuantification() == Quantifier.Quantification.ALL)
            {
                q.getBlock().setDuplicateStrategy(OutputStrategy.PERMIT);
            }
            else if (q.getQuantification() == Quantifier.Quantification.EACH
                    && (  b.getDuplicateStrategy() == OutputStrategy.PERMIT
                       || b.getDuplicateStrategy() == OutputStrategy.ENFORCE))
            {
                q.getBlock().setDuplicateStrategy(OutputStrategy.PERMIT);
            }
        }

        super.meet(b);
    }

}
