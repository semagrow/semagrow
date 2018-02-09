package org.semagrow.plan.rel.semagrow;


import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSource;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class ReactorImpl {

    static <T,K,R> Flux<R> hashJoin(Flux<T> outer,
                                Flux<T> inner,
                                final Function<T,K> outerKeySelector,
                                final Function<T,K> innerKeySelector,
                                final BiFunction<T,T,R> resultSelector,
                                final boolean generateNullsOnLeft,
                                final boolean generateNullsOnRight) {

        Mono<Map<K, Collection<T>>> lookup =  outer.collectMultimap(outerKeySelector);
        return inner.flatMap( x ->
                lookup.flatMap(map -> {
                    Collection<T> tlst = map.get(innerKeySelector.apply(x));
                    if (tlst == null) {
                        if (generateNullsOnRight)
                            return Flux.just(null);
                        else
                            return Flux.empty();
                    } else {
                        return Flux.fromIterable(tlst)
                                .map(t2 -> resultSelector.apply(x, t2));
                    }
                }));
    }

    static <T,K,R> Flux<T> mergeJoin(Flux<T> outer,
                                 Flux<T> inner,
                                 final Function<T,K> outerKeySelector,
                                 final Function<T,K> innerKeySelector,
                                 final BiFunction<T,T,R> resultSelector,
                                 final boolean generateNullsOnLeft,
                                 final boolean generateNullsOnRight) {
        return null;
    }

    static <T> Flux<T> crossProduct(Flux<T> outer,
                                    Flux<T> inner)
    {
        return null;
    }

    static <T> Flux<T> filter(Flux<T> source, Predicate<T> p) {
        return source.filter(p);
    }

    static <T> Flux<T> distinct(Flux<T> source) {
        return source.distinct();
    }

    static <T> Flux<T> intersect(Flux<T> left, Flux<T> right) {
        return null;
    }

    static <T> Flux<T> union(Flux<T> left, Flux<T> right) {
        return left.mergeWith(right);
    }

    static <T> Flux<T> except(Flux<T> left, Flux<T> right) {
        //return left(right);
        return null;
    }

    static <T,A,R> R aggregate(Flux<T> source, A seed, BiFunction<A, T, A> func,
                               Function<A, R> select){
        return null;
    }

    static <T> Flux<T> skip(Flux<T> source, long offset) {
        return source.skip(offset);
    }

    static <T> Flux<T> take(Flux<T> source, long limit) {
        return source.take(limit);
    }

    static <T> Publisher<T> toPublisher(Flux<T> flux) {
        return flux;
    }

    static <T> Flux<T> asFlux(Publisher<T> p) {
        return (p instanceof Flux) ? (Flux<T>) p : FluxSource.wrap(p);
    }

    static <T> Flux<T> empty() { return Flux.empty(); }
}
