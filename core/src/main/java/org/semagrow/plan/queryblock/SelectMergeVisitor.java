package org.semagrow.plan.queryblock;

import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

import java.util.*;

/**
 * Merges connected {@link SelectBlock}s
 * @author acharal
 */
public class SelectMergeVisitor extends AbstractQueryBlockVisitor<RuntimeException> {

    @Override
    public void meet(SelectBlock b) {
        super.meet(b);
        // traverse graph bottom up
        // start by the quantified quantifiers
        // proceed with the from quantifiers

        Collection<Quantifier> qs = new ArrayList<>(b.getQuantifiers());

        for (Quantifier q : qs)
            tryMerge(b, q);
    }

    private boolean canMerge(SelectBlock upper, Quantifier q) {

        if (q.isFrom() &&
            q.getBlock() instanceof SelectBlock)
        {
            SelectBlock lower = (SelectBlock) q.getBlock();

            boolean duplicateConstraint = !upper.hasDuplicates()
                    ||  upper.getDuplicateStrategy() == OutputStrategy.ENFORCE
                    ||  lower.getDuplicateStrategy() != OutputStrategy.ENFORCE;

            boolean ordering = lower.getOutputDataProperties().hasOrdering() &&
                               !upper.getOutputDataProperties().hasOrdering();

            boolean limit = !lower.getLimit().isPresent() && !upper.getLimit().isPresent();

            return duplicateConstraint && !ordering && limit;
        }

        return false;
    }

    /**
     * Tries to merge (if conditions apply) the quantifier {@code q}
     * to its parent {@link SelectBlock} {@code upper}.
     * @param upper
     * @param q
     */
    private void tryMerge(SelectBlock upper, Quantifier q) {

        if (canMerge(upper, q)) {

            SelectBlock lower = (SelectBlock) q.getBlock();

            // move predicates in q.getBlock() to parent
            for (Predicate p : new HashSet<>(lower.getPredicates()))
                upper.movePredicate(p);

            upper.visit(new PredicateProcessor(q));

            // move quantifiers in q.getBlock() to parent
            for (Quantifier qq : new HashSet<>(lower.getQuantifiers()))
                upper.moveQuantifier(qq);

            // process outputVariables and duplicate strategy
            processHead(upper, q);

            // remove the merged quantifier.
            upper.removeQuantifier(q);
        }
    }

    private void processHead(SelectBlock upper, Quantifier q) {

        assert q.getBlock() instanceof SelectBlock;

        SelectBlock lowerBlock = (SelectBlock) q.getBlock();

        // update projections and derived columns
        for (Map.Entry<String, ValueExpr> localVar : upper.getLocals().entrySet()) {

            ValueExpr v = localVar.getValue();

            if (v instanceof Quantifier.Var) {
                Quantifier.Var var = (Quantifier.Var) v;
                if (var.getQuantifier().equals(q)) {

                    Optional<ValueExpr> optVar = lowerBlock.getLocal(var.getName());
                    if (optVar.isPresent())
                        upper.addProjection(localVar.getKey(), optVar.get());
                    else
                        throw new RuntimeException("Bug");
                }
            } else {
                v.visit(new AbstractQueryModelVisitor<RuntimeException>() {
                    @Override
                    protected void meetNode(QueryModelNode node) throws RuntimeException {
                        if (node instanceof Quantifier.Var) {

                            Quantifier.Var v = (Quantifier.Var) node;
                            if (v.getQuantifier().equals(q)) {
                                ValueExpr var = lowerBlock.getLocal(v.getName()).orElse(v);
                                if (!var.equals(v))
                                    node.replaceWith(var);
                            }
                        }
                        super.meetNode(node);
                    }
                });
            }
        }

        if (lowerBlock.getDuplicateStrategy() == OutputStrategy.ENFORCE &&
                upper.getDuplicateStrategy() != OutputStrategy.PERMIT)
        {
            upper.setDuplicateStrategy(OutputStrategy.ENFORCE);
        }
    }


    class PredicateProcessor extends AbstractQueryBlockVisitor<RuntimeException> {

        private Quantifier q;

        private SelectBlock block;

        public PredicateProcessor(Quantifier q) {

            if (q.getBlock() instanceof SelectBlock)
                this.block = (SelectBlock)q.getBlock();
            else
                throw new IllegalArgumentException("The quantifier must refer to a SelectBlock only");

            this.q = q;
        }

        @Override
        public void meet(SelectBlock b) {
            super.meet(b);
            processPredicates(b);
        }

        /**
         * Processes predicates in a SelectBlock {@code b} that refer a quantifier {@code q}
         * to be merged. This modifies or creates new equivalent predicates in {@code b}
         * that refer to children of {@code q} instead of {@code q} itself.
         * @param b the SelectBlock that might contain predicates that involve the quantifier
         */
        private void processPredicates(SelectBlock b) {
            b.getPredicates(q).forEach(p -> processPredicate(b, p));
        }

        private void processPredicate(SelectBlock bb, Predicate p) {

            Map<Quantifier.Var, ValueExpr> replacements = new HashMap<>();

            p.getVariables().stream()
                    .filter(v -> v.getQuantifier().equals(q))
                    .forEach(v -> {
                        ValueExpr e = block.getLocal(v.getName()).orElse(v);
                        replacements.put(v, e);
                    });

            if (p instanceof InnerJoinPredicate) {

                InnerJoinPredicate jp = (InnerJoinPredicate) p;

                for (Map.Entry<Quantifier.Var, ValueExpr> entry : replacements.entrySet()) {

                    if (entry.getValue() instanceof Quantifier.Var) {
                        jp.replaceVarWith(entry.getKey(), (Quantifier.Var) entry.getValue());

                    } else {
                        ValueExpr e = jp.asExpr();
                        ThetaJoinPredicate tp = new ThetaJoinPredicate(e);
                        tp.replaceWith(entry.getKey(), entry.getValue());
                        bb.removePredicate(jp);
                        bb.addPredicate(tp);
                    }
                }

            } else if (p instanceof LeftJoinPredicate) {
                LeftJoinPredicate jp = (LeftJoinPredicate) p;

                for (Map.Entry<Quantifier.Var, ValueExpr> entry : replacements.entrySet()) {

                    if (entry.getValue() instanceof Quantifier.Var) {
                        boolean inNullProducingSide = jp.getTo().getQuantifier().equals(entry.getKey().getQuantifier());

                        Quantifier qTo = jp.getTo().getQuantifier();

                        jp.replaceVarWith(entry.getKey(), (Quantifier.Var) entry.getValue());

                        if (inNullProducingSide) {
                            SelectBlock qb = (SelectBlock) qTo.getBlock();
                            jp.getEEL().remove(q);
                            jp.getEEL().addAll(qb.getQuantifiers());
                        } else {
                            jp.getEEL().remove(q);
                            jp.getEEL().add(((Quantifier.Var) entry.getValue()).getQuantifier());
                        }

                    } else {
                        //FIXME: The case where a predicate references a complex select query
                        // and the join variable is derived. e.g.
                        // SELECT ?x ?c { { SELECT (?y * 2 AS ?c) { ?x test:num ?y } } OPTIONAL { ?x test:count ?c . }
                        /*
                        ValueExpr e = jp.asExpr();
                        ThetaJoinPredicate tp = new ThetaJoinPredicate(e);
                        tp.replaceWith(entry.getKey(), entry.getValue());
                        bb.removePredicate(jp);
                        bb.addPredicate(tp);
                        */
                    }
                }
            } else if (p instanceof ThetaJoinPredicate) {

                ThetaJoinPredicate tp = (ThetaJoinPredicate)p;

                for (Map.Entry<Quantifier.Var, ValueExpr> entry : replacements.entrySet())
                    tp.replaceWith(entry.getKey(), entry.getValue());
            }
        }
    }

}
