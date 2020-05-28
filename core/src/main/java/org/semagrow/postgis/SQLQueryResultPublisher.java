package org.semagrow.postgis;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
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
    private List<String> tables;
    
	public SQLQueryResultPublisher(ResultSet rs, List<String> tables) {
		this.rs = rs;
		this.tables = tables;
	}

	@Override
	public void subscribe(Subscriber<? super BindingSet> subscriber) {
		subscriber.onSubscribe(new SQLQuerySubscription(subscriber, rs, tables));
	}
	
	static class SQLQueryProducer implements Runnable {

        private Subscriber<? super BindingSet> subscriber;
		private ResultSet rs;
		private List<String> tables;
		private static final ValueFactory vf = SimpleValueFactory.getInstance();
		
		public SQLQueryProducer(Subscriber<? super BindingSet> subscriber, ResultSet rs, List<String> tables) {
			this.subscriber = subscriber;
			this.rs = rs;
            this.tables = tables;
        }
		
		@Override
		public void run() {
			try {
				ResultSetMetaData rsmd = rs.getMetaData();
				int columnsNumber = rsmd.getColumnCount();
				
//				if (columnsNumber > 2)
//					logger.error("Select clause requests more than two variables (id and geom)");
//				
//				while (rs.next()) {
//					QueryBindingSet result = new QueryBindingSet();
//					if (columnsNumber == 1) {
//						result.addBinding(rsmd.getColumnName(1), vf.createLiteral(rs.getString(1)));
//					}
//					else {
//						String idValue = null, idName = null, geomValue = null, geomName = null;
//						if (StringUtils.isNumeric(rs.getString(1))) {
//							idValue = rs.getString(1);
//							idName = rsmd.getColumnName(1);
//							geomValue = rs.getString(2);
//							geomName = rsmd.getColumnName(2);
//						}
//						else if (StringUtils.isNumeric(rs.getString(2))){
//							idValue = rs.getString(2);
//							idName = rsmd.getColumnName(2);
//							geomValue = rs.getString(1);
//							geomName = rsmd.getColumnName(1);
//						}
//						if (geomValue.contains("POINT")) {
//							result.addBinding(idName, vf.createLiteral("<http://deg.iit.demokritos.gr/lucas/resource/Geometry/" + idValue + ">"));
//						}
//						else if (geomValue.contains("MULTIPOLYGON")) {
//							result.addBinding(idName, vf.createLiteral("<http://deg.iit.demokritos.gr/invekos/resource/Geometry/" + idValue + ">"));
//						}
//						result.addBinding(geomName, vf.createLiteral(geomValue));
//					}
				
				while (rs.next()) {
					QueryBindingSet result = new QueryBindingSet();
					String tempColumnName = null, tempColumnValue = null;
					logger.info("columnsNumber::::::::::::::::::::::: {} ", columnsNumber);
					logger.info("tables::::::::::::::::::::::: {} ", tables.toString());
					for (int i = 1; i <= columnsNumber; i++) {
//						String columnValue = rs.getString(i);
						if (rsmd.getColumnClassName(i).equals("java.lang.Integer")) {
//							if (columnValue)
							logger.info("string is numeric!!!: {} ", rsmd.getColumnName(i));
							logger.info("tables[{}]: {} ", i-1, tables.get(i-1));
							if (tables.get(i-1).equals("?")) {
								tempColumnName = rsmd.getColumnName(i);
								tempColumnValue = rs.getString(i);
							} 
							else {
								result.addBinding(rsmd.getColumnName(i), vf.createLiteral("<http://deg.iit.demokritos.gr/" + tables.get(i-1) + "/resource/Geometry/" + rs.getString(i) + ">"));
							}
							continue;
						}
						if (tempColumnName != null && tempColumnValue != null) {
							logger.info("tables[{}]: {} ", i-1, tables.get(i-1));
							if (rs.getString(i).contains("POINT")) {
								result.addBinding(tempColumnName, vf.createLiteral("<http://deg.iit.demokritos.gr/lucas/resource/Geometry/" + tempColumnValue + ">"));
							}
							else if (rs.getString(i).contains("MULTIPOLYGON")) {
								result.addBinding(tempColumnName, vf.createLiteral("<http://deg.iit.demokritos.gr/invekos/resource/Geometry/" + tempColumnValue + ">"));
							}
							tempColumnName = tempColumnValue = null;
						}
//						logger.info("columnClassName:: {} ", rsmd.getColumnClassName(i));
//						logger.info("columnLabel:: {} ", rsmd.getColumnLabel(i));
//						logger.info("SchemaName:: {} ", rsmd.getSchemaName(i));
//						logger.info("TableName:: {} ", rsmd.getTableName(i));
//						logger.info("CatalogName:: {} ", rsmd.getCatalogName(i));
						logger.info("columnName:: {} ", rsmd.getColumnName(i));
						logger.info("columnValue:: {} ", rs.getString(i));
						result.addBinding(rsmd.getColumnName(i), vf.createLiteral(rs.getString(i)));
						logger.info(" {} as {} ", rs.getString(i), rsmd.getColumnName(i));
					}
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
		private List<String> tables;
		private Future<?> f;
		
		public SQLQuerySubscription(Subscriber<? super BindingSet> subscriber, ResultSet rs, List<String> tables) {
			this.subscriber = subscriber; 
			this.rs = rs;
			this.tables = tables;
		}
		
		@Override
		public void request(long l) {
			if (producer == null) {
				producer = new SQLQueryProducer(subscriber, rs, tables);
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
