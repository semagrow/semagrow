package eu.semagrow.core.impl.evalit.iteration;

import info.aduna.iteration.Iteration;
import info.aduna.iteration.LookAheadIteration;

import java.util.*;

/**
 * Created by angel on 9/26/14.
 */
public class MergeUnionIterator<E,X extends Exception> extends LookAheadIteration<E,X> {

    private Comparator<Entry> comparator;
    private Iterable<? extends Iteration<? extends E, X>> argIters;

    private PriorityQueue<Entry> queue;
    // private PriorityBlockingQueue<Entry> queue;  // thread-safe

    private Iteration<? extends E, X> probeIter = null;

    private boolean initialized = false;

    public MergeUnionIterator(Comparator<E> comparator, Iteration<? extends E, X>... args) {
        this(comparator, Arrays.asList(args));
    }

    public MergeUnionIterator(Comparator<E> comparator, Iterable<? extends Iteration<? extends E, X>> args) {
        this.comparator = new EntryComparator(comparator);
        argIters = args;

        queue = new PriorityQueue<Entry>(10, this.comparator);
    }

    @Override
    protected E getNextElement() throws X {

        if (!initialized) {
            init();
            initialized = true;
        }


        if (probeIter != null) {
            if (probeIter.hasNext()) {
                E v = probeIter.next();
                queue.add(new Entry(v, probeIter));
            }
            probeIter = null;
        }

        if (queue.isEmpty())
            return null;

        Entry entry = queue.poll();
        probeIter = entry.getIteration();
        return entry.getValue();
    }

    protected void init() throws X {
        for (Iteration<? extends E, X> iter : argIters)  {
            if (iter.hasNext())
                queue.add(new Entry(iter.next(), iter));
        }
    }

    private class Entry {
        private E val;
        private Iteration<? extends E, X> iter;

        public Entry(E val, Iteration<? extends E, X> iter) {
            this.val = val;
            this.iter = iter;
        }

        public E getValue() { return val; }
        public Iteration<? extends E, X> getIteration() { return iter; }
    }

    private class EntryComparator implements Comparator<Entry> {

        private Comparator<E> comparator;

        public EntryComparator(Comparator<E> comparator) { this.comparator = comparator; }

        public Comparator<E> getValueComparator() { return comparator; }

        public int compare(Entry entry, Entry entry2) {
            return comparator.compare(entry.getValue(), entry2.getValue());
        }

        public boolean equals(Object o) {
            return false;
        }
    }
}
