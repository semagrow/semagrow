package org.semagrow.util;

import java.util.*;

/**
 * @author acharal
 */
public class PartitionedSet<T> {

    class PartitionElem {

        private T payload;

        private PartitionElem parent;

        private int rank;

        public PartitionElem(T t) {
            this.payload = t;
            setParent(this);
        }

        void setPayload(T t) {
            this.payload = t;
        }

        T getPayload() {
            return payload;
        }

        void setParent(PartitionElem elem) {
            this.parent = elem;
            if (this.parent == this)
                rank = 0;
        }

        public PartitionElem getParent() {
            return parent;
        }

        public int getRank() { return rank; }

        public void setRank(int rank){ this.rank = rank; }

        public boolean isRoot() {
            return getParent() == this;
        }

    }

    private Map<T, PartitionElem> map = new HashMap<>();

    private Set<PartitionElem> partitionElems = new HashSet<>();

    public PartitionedSet(Collection<T> elements) {
        for (T e : elements) {
            PartitionElem partElem = make(e);
            this.partitionElems.add(partElem);
        }
    }

    private PartitionElem make(T t) {
        PartitionElem elem = new PartitionElem(t);
        map.put(t, elem);
        return elem;
    }

    protected PartitionElem find(T t) {
        PartitionElem elem = map.get(t);
        return find(elem);
    }

    private PartitionElem find(PartitionElem elem) {
        assert elem != null;
        PartitionElem elem1 = elem;

        while (!elem1.isRoot()) {
            elem1 = elem1.getParent();
        }
        elem.setParent(elem1);
        return elem1;
    }

    public T rep(T t) { return find(t).getPayload(); }

    public void union(T p1, T p2) {
        PartitionElem elem1 = map.get(p1);
        PartitionElem elem2 = map.get(p2);
        union(elem1, elem2);
    }

    private void union(PartitionElem e1, PartitionElem e2) {
        assert e1 != null && e2 != null;
        PartitionElem r1 = find(e1);
        PartitionElem r2 = find(e2);

        if (r1.getRank() < r2.getRank()) {
            r1.setParent(r2);
        } else if (r1.getRank() > r2.getRank()) {
            r2.setParent(r1);
        } else if (!r1.equals(r2)) {
            r2.setParent(r1);
            r1.setRank(r1.getRank() + 1);
        }
    }

    public boolean areEqual(T e1, T e2) {
        return find(e1) == find(e2);
    }

    public boolean areEqual(PartitionElem e1, PartitionElem e2) {
        return find(e1) == find(e2);
    }


    public Collection<Set<T>> getPartitions() {
        Map<PartitionElem, Set<T>> col = new HashMap<>();

        for (PartitionElem elem : partitionElems) {
            PartitionElem root = find(elem);
            Set<T> c = col.getOrDefault(root, new HashSet<>());
            c.add(elem.payload);
            col.put(root, c);
        }

        return col.values();
    }

    public Set<T> getEquals(T e) {
        PartitionElem pe = find(e);
        Set<T> eq = new HashSet<T>();

        for (PartitionElem elem : partitionElems) {
            if (areEqual(elem,pe))
                eq.add(elem.payload);
        }
        return eq;
    }
}
