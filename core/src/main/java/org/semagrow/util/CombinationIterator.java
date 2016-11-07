package org.semagrow.util;

import java.util.*;

/**
 * Created by angel on 29/9/2016.
 */
public class CombinationIterator<E> implements Iterator<Collection<E>>, Iterable<Collection<E>> {

    private int[] indices;
    private final int[] maxIndices;
    private final Object[] elements;

    public CombinationIterator(int number, Collection<? extends E> items) {

        if (items == null || number < 1 || items.size() < number) {
            throw new IllegalArgumentException("|items| >= number && number > 1");
        }

        elements = items.toArray();
        indices = new int[number];
        maxIndices = new int[number];

        for (int i = 0; i < number; i++) {
            indices[i] = i;
            maxIndices[i] = elements.length + i - indices.length;
        }
    }

    public Iterator<Collection<E>> iterator() {
        return this;
    }

    public boolean hasNext() {
        return indices != null;
    }

    @SuppressWarnings("unchecked")
    private Set<E> createFromIndices() {
        List<E> result = new ArrayList<E>(indices.length * 2);
        for (int i = 0; i < indices.length; i++) {
            result.add((E) elements[indices[i]]);
        }
        return new HashSet<E>(result);
    }

    private void incrementIndices() {
        if (indices[0] == maxIndices[0]) {
            indices = null;
            return;
        }
        for (int i = indices.length - 1; i >= 0; i--) {
            if (indices[i] != maxIndices[i]) {
                int val = ++indices[i];
                for (int j = i + 1; j < indices.length; j++) {
                    indices[j] = ++val;
                }
                break;
            }
        }
    }

    public Collection<E> next() {
        if (indices == null) {
            throw new NoSuchElementException("End of iterator");
        }
        Collection<E> result = createFromIndices();
        incrementIndices();
        return result;
    }

    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
