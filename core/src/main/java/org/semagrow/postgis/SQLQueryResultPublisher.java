package org.semagrow.postgis;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLQueryResultPublisher implements Publisher<BindingSet> {

    private static final Logger logger = LoggerFactory.getLogger(SQLQueryResultPublisher.class);

    private static ExecutorService executorService = Executors.newCachedThreadPool();
    
    private ResultSet rs;
    
	public SQLQueryResultPublisher(ResultSet rs) {
		this.rs = rs;
	}

	@Override
	public void subscribe(Subscriber<? super BindingSet> subscriber) {
		subscriber.onSubscribe(new SQLQuerySubscription(subscriber, rs));
	}
	
	static class SQLQueryProducer implements Runnable {

        private Subscriber<? super BindingSet> subscriber;
		private ResultSet rs;
		private long requested = 0;
        private Boolean shutdownFlag = false;
        private CountDownLatch latch = new CountDownLatch(0);
		private static final ValueFactory vf = SimpleValueFactory.getInstance();
        final private Object syncRequest = new Object();
		
		public SQLQueryProducer(Subscriber<? super BindingSet> subscriber, ResultSet rs) {
            this.rs = rs;
            this.subscriber = subscriber;
        }
		
		public void shutdown() {
            shutdownFlag = true;
        }
		
		public void requestMore(long n) {
            latch.countDown();
            synchronized (syncRequest) {
                requested += n;
            }
        }
		
		public void fullfillOne() {
            synchronized (syncRequest) {
                assert requested > 0;
                requested--;
            }
        }
		
		public void awaitForRequests() throws InterruptedException {
            synchronized (syncRequest) {
                if (requested == 0)
                    latch.await();
            }
        }
		
		@Override
		public void run() {
			
			try {
				awaitForRequests();
				ResultSetMetaData rsmd = rs.getMetaData();
				int columnsNumber = rsmd.getColumnCount();
				while (rs.next()) {
					QueryBindingSet result = new QueryBindingSet();
					awaitForRequests();
					for (int i = 1; i <= columnsNumber; i++) {
						String columnValue = rs.getString(i);
						logger.info("columnName:: {} ", rsmd.getColumnName(i));
						logger.info("columnValue:: {} ", columnValue);
						result.addBinding(rsmd.getColumnName(i), vf.createLiteral(columnValue));
						logger.info(" {} as {} ", columnValue, rsmd.getColumnName(i));
					}
					fullfillOne();
					subscriber.onNext(result);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (QueryEvaluationException e) {
                logger.warn("Error while evaluating subquery", e);
                subscriber.onError(e);
			} catch (InterruptedException i) {
                if (shutdownFlag)
                    logger.info("Subscription shutdown by subscriber. Interrupted.");
                else
                    logger.warn("SubQuery thread interrupted.");
            }
			subscriber.onComplete();
		}
		
	}
	
	public static final class SQLQuerySubscription implements Subscription {
		
		private Subscriber<? super BindingSet> subscriber;
		private SQLQueryProducer producer;
		private ResultSet rs;
		private Future<?> f;
		
		public SQLQuerySubscription(Subscriber<? super BindingSet> subscriber, ResultSet rs) {
			this.subscriber = subscriber; 
			this.rs = rs;
		}
		
		@Override
		public void request(long l) {
			if (producer == null) {
				producer = new SQLQueryProducer(subscriber, rs);
                //executorService.execute(producer);
                Future<?> f = executorService.submit(producer);
                producer.requestMore(l);
			}
			else {
				producer.requestMore(l);
			}
		}

		@Override
		public void cancel() {
			if (producer != null) {
                producer.shutdown();
                assert f != null;
                if (!f.isDone() && !f.isCancelled())
                    f.cancel(true);
            }
		}
	}
}
