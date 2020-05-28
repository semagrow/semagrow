package org.semagrow.postgis;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
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
		private static final ValueFactory vf = SimpleValueFactory.getInstance();
		
		public SQLQueryProducer(Subscriber<? super BindingSet> subscriber, ResultSet rs) {
            this.rs = rs;
            this.subscriber = subscriber;
        }
		
		@Override
		public void run() {
			try {
				ResultSetMetaData rsmd = rs.getMetaData();
				int columnsNumber = rsmd.getColumnCount();
				if (columnsNumber > 2)
					logger.error("Select clause requests more than two variables (id and geom)");
				
				while (rs.next()) {
					QueryBindingSet result = new QueryBindingSet();
					if (columnsNumber == 1) {
						result.addBinding(rsmd.getColumnName(1), vf.createLiteral(rs.getString(1)));
					}
					else {
						String idValue = null, idName = null, geomValue = null, geomName = null;
						if (StringUtils.isNumeric(rs.getString(1))) {
							idValue = rs.getString(1);
							idName = rsmd.getColumnName(1);
							geomValue = rs.getString(2);
							geomName = rsmd.getColumnName(2);
						}
						else if (StringUtils.isNumeric(rs.getString(2))){
							idValue = rs.getString(2);
							idName = rsmd.getColumnName(2);
							geomValue = rs.getString(1);
							geomName = rsmd.getColumnName(1);
						}
						if (geomValue.contains("POINT")) {
							result.addBinding(idName, vf.createLiteral("<http://deg.iit.demokritos.gr/lucas/resource/Geometry/" + idValue + ">"));
						}
						else if (geomValue.contains("MULTIPOLYGON")) {
							result.addBinding(idName, vf.createLiteral("<http://deg.iit.demokritos.gr/invekos/resource/Geometry/" + idValue + ">"));
						}
						result.addBinding(geomName, vf.createLiteral(geomValue));
					}
//					for (int i = 1; i <= columnsNumber; i++) {
//						String columnValue = rs.getString(i);
//						if (StringUtils.isNumeric(columnValue)) {
////							if (columnValue)
//							logger.info("string is numeric!!!: {} ", rsmd.getColumnName(i));
//						}
//						logger.info("columnName:: {} ", rsmd.getColumnName(i));
//						logger.info("columnValue:: {} ", columnValue);
//						result.addBinding(rsmd.getColumnName(i), vf.createLiteral(columnValue));
//						logger.info(" {} as {} ", columnValue, rsmd.getColumnName(i));
//					}
					subscriber.onNext(result);
				}
			} catch (SQLException e) {
				logger.warn("Error while reading resultSet data", e);
				e.printStackTrace();
			} catch (QueryEvaluationException e) {
                logger.warn("Error while evaluating subquery", e);
                subscriber.onError(e);
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
                f = executorService.submit(producer);
			}
		}

		@Override
		public void cancel() {
			if (producer != null) {
                assert f != null;
                if (!f.isDone() && !f.isCancelled())
                    f.cancel(true);
            }
		}
	}
}
