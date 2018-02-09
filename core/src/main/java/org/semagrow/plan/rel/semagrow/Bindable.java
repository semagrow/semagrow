package org.semagrow.plan.rel.semagrow;

import org.reactivestreams.Publisher;

import java.util.Map;

public interface Bindable<T> {

    Publisher<T> bind(DataContext<T> ctx);

    class DataContext<T> {

        /**
         * Map that contains the name of the correlated variable and
         * a publisher that contains none or more bindings for that variable.
         */
        Map<String, Publisher<T>> bindings;

        static <T> DataContext<T> empty() {
            DataContext ctx = new DataContext<T>();
            return ctx;
        }
    }
}
