package eu.semagrow.core.impl;

import eu.semagrow.core.impl.evalit.iteration.MergeUnionIterator;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iterations;
import junit.framework.TestCase;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;

import java.util.*;

public class MergeUnionIteratorTest extends TestCase {

    public void testBasic() throws Exception {

        List<Integer> aa = Arrays.asList(1,2,3,4,5);
        List<Integer> bb = Arrays.asList(2,5,4,9,0);

        Collections.sort(aa, Collections.reverseOrder());
        Collections.sort(bb, Collections.reverseOrder());

        CloseableIteration<Integer,Exception> iter = new MergeUnionIterator<Integer,Exception>(Collections.<Integer>reverseOrder(),
                new CollectionIteration<Integer,Exception>(aa),
                new CollectionIteration<Integer,Exception>(bb));

        List<Integer> concat = new ArrayList<Integer>(aa);
        concat.addAll(bb);

        Collections.sort(concat, Collections.reverseOrder());
        assertEquals(concat, Iterations.asList(iter));
    }

    public void testOneEmpty() throws Exception {
        List<Integer> aa = Arrays.asList(1,2,3,4,5);
        List<Integer> bb = Arrays.asList();

        Collections.sort(aa, Collections.reverseOrder());
        Collections.sort(bb, Collections.reverseOrder());

        CloseableIteration<Integer,Exception> iter = new MergeUnionIterator<Integer,Exception>(Collections.<Integer>reverseOrder(),
                new CollectionIteration<Integer,Exception>(aa),
                new CollectionIteration<Integer,Exception>(bb));

        assertEquals(aa, Iterations.asList(iter));
    }

    public void testAllEmpty() throws Exception {
        List<Integer> aa = Arrays.asList();
        List<Integer> bb = Arrays.asList();

        Collections.sort(aa, Collections.reverseOrder());
        Collections.sort(bb, Collections.reverseOrder());

        CloseableIteration<Integer,Exception> iter = new MergeUnionIterator<Integer,Exception>(Collections.<Integer>reverseOrder(),
                new CollectionIteration<Integer,Exception>(aa),
                new CollectionIteration<Integer,Exception>(bb));

        assertTrue(Iterations.asList(iter).isEmpty());
    }
}