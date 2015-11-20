package eu.semagrow.core.impl.evaluation.rx.reactor;

import org.slf4j.MDC;
//import reactor.core.processor.InsufficientCapacityException;
import reactor.fn.Consumer;
import reactor.core.Dispatcher;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by angel on 2/10/2015.
 */
public class MDCAwareDispatcher implements Dispatcher {

    private Dispatcher delegate;
    private final Map<String,String> contextMap;

    public MDCAwareDispatcher(Dispatcher delegate) {
        this.delegate = delegate;
        contextMap = MDC.getCopyOfContextMap();
    }

    @Override
    public <E> void dispatch(E e, final Consumer<E> consumer, Consumer<Throwable> consumer1) {

        delegate.dispatch(e, new Consumer<E>() {
            @Override
            public void accept(E e) {
                MDC.setContextMap(contextMap);
                consumer.accept(e);
            }
        }, new Consumer<Throwable>() {
            @Override
            public void accept(Throwable e) {
                MDC.setContextMap(contextMap);
                consumer1.accept(e);
            }
        });
    }

    @Override
    public <E> void tryDispatch(E e, Consumer<E> consumer, Consumer<Throwable> consumer1) /*throws InsufficientCapacityException*/ {

        delegate.tryDispatch(e, new Consumer<E>() {
            @Override
            public void accept(E e) {
                MDC.setContextMap(contextMap);
                consumer.accept(e);
            }
        }, new Consumer<Throwable>() {
            @Override
            public void accept(Throwable e) {
                MDC.setContextMap(contextMap);
                consumer1.accept(e);
            }
        });

    }

    @Override
    public long remainingSlots() {
        return delegate.remainingSlots();
    }

    @Override
    public long backlogSize() {
        return delegate.backlogSize();
    }

    @Override
    public boolean supportsOrdering() {
        return delegate.supportsOrdering();
    }

    @Override
    public boolean inContext() {
        return delegate.inContext();
    }

    @Override
    public void execute(Runnable command) {

        delegate.execute(new Runnable() {
            @Override
            public void run() {
                MDC.setContextMap(contextMap);
                command.run();
            }
        });
    }

    @Override
    public boolean alive() {
        return delegate.alive();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public boolean awaitAndShutdown() {
        return delegate.awaitAndShutdown();
    }

    @Override
    public boolean awaitAndShutdown(long l, TimeUnit timeUnit) {
        return delegate.awaitAndShutdown(l, timeUnit);
    }

    @Override
    public void forceShutdown() {
        delegate.forceShutdown();
    }
}
