package eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration;

import eu.semagrow.stack.modules.sails.semagrow.algebra.ProvenanceValue;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.EvaluationStrategyImpl;
//import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.parallel.ParallelEvaluator;
//import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.parallel.base.ParallelEvaluatorBase;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;
import info.aduna.iteration.LookAheadIteration;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import java.util.*;

/**
 * Created by angel on 9/26/14.
 */
public class MergeJoinIteration extends LookAheadIteration<BindingSet, QueryEvaluationException> {

    private CloseableIteration<BindingSet, QueryEvaluationException> leftIter;

    private CloseableIteration<BindingSet, QueryEvaluationException> rightIter;

    private Set<String> joinAttributes;

    private Comparator<BindingSet> comparator;

    private BindingSet leftBindings;

    private BindingSet rightBindings;

    private Collection<BindingSet> leftBuf;

    private Collection<BindingSet> rightBuf;

    private CloseableIteration<BindingSet,QueryEvaluationException> bufIter;

    //private ParallelEvaluator evaluator = null;
    
    public MergeJoinIteration(Comparator<BindingSet> comparator,
                              EvaluationStrategy evaluationStrategy,
                              Join join, BindingSet bindings)
            throws QueryEvaluationException {

        this.comparator = comparator;
        /*
        evaluator = new ParallelEvaluatorBase(evaluationStrategy, bindings, join);        
        CompletableFuture cfLeft = evaluator.getLeftArgCompletableFuture();
        CompletableFuture cfRight = evaluator.getRightArgCompletableFuture();
        CompletableFuture.allOf(cfLeft,cfRight).thenRun(()->{
            joinAttributes = join.getLeftArg().getBindingNames();
            joinAttributes.retainAll(join.getRightArg().getBindingNames());
            try {
                leftIter = (CloseableIteration<BindingSet, QueryEvaluationException>) cfLeft.get();
                rightIter = (CloseableIteration<BindingSet, QueryEvaluationException>) cfRight.get();                
            } catch ( InterruptedException | ExecutionException ex) {}
        });
        */
        leftIter = evaluationStrategy.evaluate(join.getLeftArg(), bindings);
        rightIter = evaluationStrategy.evaluate(join.getRightArg(), bindings);
        bufIter = new EmptyIteration<BindingSet, QueryEvaluationException>();

        leftBuf = new LinkedList<BindingSet>();
        rightBuf = new LinkedList<BindingSet>();
    }

    @Override
    protected BindingSet getNextElement() throws QueryEvaluationException {

        if (bufIter.hasNext())
            return bufIter.next();

        if (!leftIter.hasNext() && !rightIter.hasNext())
            return null;

        if (leftBindings == null)
            leftBindings = leftIter.next();

        if (rightBindings == null)
            rightBindings = rightIter.next();

        BindingSet leftKey  = calcKey(leftBindings, joinAttributes);
        BindingSet rightKey = calcKey(rightBindings, joinAttributes);

        while (leftIter.hasNext() && rightIter.hasNext()) {
            int comp_res = comparator.compare(leftKey, rightKey);

            if (comp_res < 0) {
                leftBindings = leftIter.next();
                leftKey = calcKey(leftBindings,joinAttributes);
            } else if (comp_res > 0) {
                rightBindings = rightIter.next();
                rightKey = calcKey(rightBindings,joinAttributes);
            }
            else {
                // two iteration in sync (leftKey=rightKey)
                break;
            }
        }

        // fill left buffer with rows with the same key
        leftBuf = new LinkedList<BindingSet>();
        while (leftIter.hasNext()) {
            leftBindings = leftIter.next();
            if (comparator.compare(leftKey, calcKey(leftBindings, joinAttributes)) == 0)
                leftBuf.add(leftBindings);
            else
                break;
        }

        // fill right buffer with rows with the same key
        rightBuf = new LinkedList<BindingSet>();
        while (rightIter.hasNext()) {
            rightBindings = rightIter.next();
            if (comparator.compare(leftKey, calcKey(rightBindings, joinAttributes)) == 0)
                rightBuf.add(rightBindings);
            else
                break;
        }

        // create the crossproduct of the buffers
        if (!leftBuf.isEmpty() || !rightBuf.isEmpty()) {
            bufIter = new CrossProductIteration(leftBuf, rightBuf);
            if (bufIter.hasNext())
                return bufIter.next();
        }
        return null;
    }

    @Override
    protected void handleClose()
            throws QueryEvaluationException
    {
        super.handleClose();

        leftIter.close();
        rightIter.close();
        bufIter.close();
    }

    private BindingSet calcKey(BindingSet bindings, Set<String> commonVars) {
        QueryBindingSet q = new QueryBindingSet();
        for (String varName : commonVars) {
            Binding b = bindings.getBinding(varName);
            if (b != null) {
                q.addBinding(b);
            }
        }
        return q;
    }

    protected BindingSet combineBindings(BindingSet b1, BindingSet b2) {
        QueryBindingSet result = new QueryBindingSet();

        if (b1.hasBinding(EvaluationStrategyImpl.provenanceField) &&
                b2.hasBinding(EvaluationStrategyImpl.provenanceField)) {
            ProvenanceValue p1 = (ProvenanceValue)b1.getBinding(EvaluationStrategyImpl.provenanceField).getValue();
            ProvenanceValue p2 = (ProvenanceValue)b2.getBinding(EvaluationStrategyImpl.provenanceField).getValue();
            ProvenanceValue p = new ProvenanceValue(p1);
            p.merge(p2);
            result.addBinding(EvaluationStrategyImpl.provenanceField, p);
        }

        for (Binding b : b1) {
            if (!result.hasBinding(b.getName()))
                result.addBinding(b);
        }

        for (String name : b2.getBindingNames()) {
            Binding b = b2.getBinding(name);
            if (!result.hasBinding(name)) {
                result.addBinding(b);
            }
        }

        return result;
    }

    protected class CrossProductIteration extends LookAheadIteration<BindingSet, QueryEvaluationException> {

        protected final Iterable<BindingSet> inputBindings;
        protected final Iterable<BindingSet> resultIterable;

        protected Iterator<BindingSet> inputBindingsIterator = null;
        protected Iterator<BindingSet> resultIterator = null;
        protected BindingSet currentInputBinding = null;

        public CrossProductIteration(
                Iterable<BindingSet> resultIteration,
                Iterable<BindingSet> inputBindings) {
            super();
            this.resultIterable = resultIteration;
            this.inputBindings = inputBindings;
            this.resultIterator = resultIterable.iterator();
        }

        @Override
        protected BindingSet getNextElement() throws QueryEvaluationException {

            if (currentInputBinding==null) {
                inputBindingsIterator = inputBindings.iterator();
                if (resultIterator.hasNext())
                    currentInputBinding = resultIterator.next();
                else
                    return null;  // no more results
            }

            if (inputBindingsIterator.hasNext()) {
                BindingSet next = inputBindingsIterator.next();
                BindingSet res = combineBindings(currentInputBinding, next);
                if (!inputBindingsIterator.hasNext())
                    currentInputBinding = null;
                return res;
            }

            return null;
        }
    }
}
