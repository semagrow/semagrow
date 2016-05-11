package eu.semagrow.cassandra.utils;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by antonis on 7/4/2016.
 */
public final class Utils {

    public static <T> Collector<T, ?, T> singletonCollector() {
        return Collectors.collectingAndThen(
                Collectors.toList(),
                list -> {
                    if (list.size() == 0) {
                        return null;
                    }
                    if (list.size() > 1) {
                        throw new IllegalStateException();
                    }
                    return list.get(0);
                }
        );
    }

    public static <T> Set<T> union(Set<T> p, Set<T> q) {
        Set<T> result = new HashSet<>(p);
        p.addAll(q);
        return result;
    }

    public static <T> Set<T> intersection(Collection<T> p, Collection<T> q) {
        Set<T> result = new HashSet<>(p);
        result.retainAll(q);
        return result;
    }
}
