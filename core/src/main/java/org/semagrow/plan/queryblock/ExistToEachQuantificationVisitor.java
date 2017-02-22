package org.semagrow.plan.queryblock;

import java.util.Collection;

/**
 * Converts an existential quantification into an each quantification.
 *
 * This transformation is useful for nested queries.
 *
 * @author acharal
 */
public class ExistToEachQuantificationVisitor extends AbstractQueryBlockVisitor<RuntimeException> {


    @Override
    public void meet(SelectBlock block) {

        super.meet(block);

        Collection<Quantifier> qs = block.getQuantifiers();

        if (block.getDuplicateStrategy() == OutputStrategy.PERMIT
            || block.getDuplicateStrategy() == OutputStrategy.ENFORCE) {

            for (Quantifier q : qs) {
                if (q.getQuantification() == Quantifier.Quantification.ANY) {
                    q.setQuantification(Quantifier.Quantification.EACH);
                    block.setDuplicateStrategy(OutputStrategy.ENFORCE);
                }
            }
        }

    }
}
