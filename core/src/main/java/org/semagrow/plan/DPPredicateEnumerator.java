package org.semagrow.plan;


import java.util.*;
import java.util.stream.Stream;

/**
 *
 * @author acharal
 */
public class DPPredicateEnumerator<T,P> {

    private PlanGenerator<T,P> generator;

    public DPPredicateEnumerator(PlanGenerator<T,P> generator) {
        this.generator = generator;
    }

    public interface PlanGenerator<T,P> {

        Pair<Collection<T>, Collection<P>> access(T t);

        Pair<Collection<T>, Collection<P>> compose(Pair<Collection<T>, Collection<P>> p1, Pair<Collection<T>, Collection<P>> p2);

        Iterator<Pair<Collection<T>, Collection<T>>> enumerate(Collection<T> tt);

        void prune(Collection<P> plans);
    }

    public Collection<P> enumerate(Collection<T> aa) {

        Map<Collection<T>, Pair<Collection<T>, Collection<P>>> dpTbl = new HashMap<>();

        Stream<Pair<Collection<T>, Collection<P>>> pp = aa.stream().map(generator::access);

        pp.forEach(e -> dpTbl.put(e.getFirst(), e));

        Iterator<Pair<Collection<T>, Collection<T>>> it = generator.enumerate(aa);
        while (it.hasNext()) {
            Pair<Collection<T>, Collection<T>> p = it.next();

            Pair<Collection<T>, Collection<P>> f = dpTbl.get(p.getFirst());
            Pair<Collection<T>, Collection<P>> s = dpTbl.get(p.getSecond());

            Pair<Collection<T>, Collection<P>> cp = generator.compose(f, s);

            Pair<Collection<T>, Collection<P>> optCol = dpTbl.get(cp.getFirst());
            if (optCol == null)
                optCol = cp;
            else
                optCol.getSecond().addAll(cp.getSecond());

            generator.prune(optCol.getSecond());

            dpTbl.put(cp.getFirst(), optCol);
        }

        return dpTbl.get(aa).getSecond();
    }

}
