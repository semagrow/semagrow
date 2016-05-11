package eu.semagrow.core.impl.evalit.iteration;

import java.util.ArrayList;
import java.util.Set;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.LookAheadIteration;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Created by antru on 2/9/15.
 */

public class UnionJoinIteration extends LookAheadIteration<BindingSet, QueryEvaluationException> {

	private CloseableIteration<BindingSet, QueryEvaluationException> leftIter;
	private CloseableIteration<BindingSet, QueryEvaluationException> rightIter;
	private ArrayList<BindingSet> leftList;
	private Set<String> joinAttributes;

    public UnionJoinIteration(CloseableIteration<BindingSet, QueryEvaluationException> leftIter,
		            CloseableIteration<BindingSet, QueryEvaluationException> rightIter,
		            Set<String> attributes) throws QueryEvaluationException {
		
		this.leftIter = leftIter;
		this.rightIter = rightIter;
		this.joinAttributes = attributes;
		
		leftList = new ArrayList<BindingSet>();
		while (leftIter.hasNext())
			leftList.add(leftIter.next());
        leftIter.close();
	}
    
	@Override
	protected BindingSet getNextElement() throws QueryEvaluationException {
		
		while (rightIter.hasNext()) {
            int i=-1;
            QueryBindingSet joinBindings = new QueryBindingSet();
			BindingSet rightBindings = rightIter.next();
			
			for (Binding b : rightBindings) {
				// get the relevant left binding
				String bName = b.getName();
				int splitPoint = bName.lastIndexOf("_");
				i = Integer.parseInt(bName.substring(splitPoint+1)) - 1;

                // create new Binding
				joinBindings.addBinding(bName.substring(0,splitPoint),b.getValue()); 
			}

			for (Binding b : leftList.get(i)) {
                if (!joinBindings.hasBinding(b.getName()))
     				joinBindings.addBinding(b);
			}
			return joinBindings;
		}
        rightIter.close();
		return null;
	}

		    
}
