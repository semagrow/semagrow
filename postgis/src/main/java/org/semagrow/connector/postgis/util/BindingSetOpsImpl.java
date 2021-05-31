package org.semagrow.connector.postgis.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.jooq.Record;
import org.semagrow.evaluation.BindingSetOps;
import org.semagrow.evaluation.reactor.FederatedEvaluationStrategyImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BindingSetOpsImpl implements BindingSetOps {

	private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);
	private static final ValueFactory vf = SimpleValueFactory.getInstance();

    public static final BindingSet transform(Record r, String dbname, Map<String,String> bindingVars) throws SQLException {

        ResultSet rs = r.intoResultSet();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        QueryBindingSet result = new QueryBindingSet();
        logger.debug("columnsNumber: {} ", columnsNumber);
        for (int i = 0; i < columnsNumber; i++) {
        	logger.debug("i: {} ", i);
            if (rsmd.getColumnClassName(i+1).equals("java.lang.Integer")) {
                logger.debug("string is numeric!!!: {} ", rsmd.getColumnName(i+1));
                String id = "http://rdf.semagrow.org/pgm/" + dbname + "/resource/" + r.getValue(i);
                result.addBinding(rsmd.getColumnName(i+1), vf.createIRI(id));
            }
            else if (rsmd.getColumnClassName(i+1).equals("java.lang.String")) {
            	String wkt = "\"" + (String) r.getValue(i) + "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
            	result.addBinding(rsmd.getColumnName(i+1), vf.createLiteral(wkt));
            }
            else if (rsmd.getColumnClassName(i+1).equals("java.lang.Double")) {
            	String distance = "\"" + (Double) r.getValue(i) + "\"^^<http://www.w3.org/2001/XMLSchema#double>";
            	result.addBinding(rsmd.getColumnName(i+1), vf.createLiteral(distance));
            }
            else {
            	logger.error("java.lang.ClassCastException: {} is {}", rsmd.getColumnName(i+1), rsmd.getColumnClassName(i+1));
            	throw new SQLException();
            }
            logger.debug(" {} as {} ", r.getValue(i), rsmd.getColumnName(i+1));
        }
        
        for (Map.Entry<String,String> binding : bindingVars.entrySet()) {
        	result.addBinding(binding.getKey(), vf.createIRI(binding.getValue()));
        }
        return result;
    }
    
//    public static final BindingSet transform(Record r, List<String> tables) throws SQLException {
//
//        ResultSet rs = r.intoResultSet();
//        ResultSetMetaData rsmd = rs.getMetaData();
//        int columnsNumber = rsmd.getColumnCount();
//
//        QueryBindingSet result = new QueryBindingSet();
//        String tempColumnName = null, tempColumnValue = null;
//        logger.debug("columnsNumber::::::::::::::::::::::: {} ", columnsNumber);
//        logger.debug("tables::::::::::::::::::::::: {} ", tables.toString());
//        for (int i = 0; i < columnsNumber; i++) {
//            if (rsmd.getColumnClassName(i+1).equals("java.lang.Integer")) {
//                logger.debug("string is numeric!!!: {} ", rsmd.getColumnName(i+1));
//                logger.debug("tables[{}]: {} ", i, tables.get(i));
//                if (tables.get(i).equals("?")) {
//                    tempColumnName = rsmd.getColumnName(i+1);
//                    tempColumnValue = r.getValue(i) instanceof String ? (String) r.getValue(i) : r.getValue(i).toString();
//                } else {
//                    result.addBinding(
//                            rsmd.getColumnName(i+1),
//                            vf.createIRI("http://deg.iit.demokritos.gr/" + tables.get(i) + "/geometry/" + r.getValue(i) + "")
//                    );
//                }
//                continue;
//            }
//            if (tempColumnName != null && tempColumnValue != null) {
//                logger.debug("tables[{}]: {} ", i, tables.get(i));
//                if (((String) r.getValue(i)).contains("POINT")) {
//                    result.addBinding(tempColumnName, vf.createIRI("http://deg.iit.demokritos.gr/lucas/geometry/" + tempColumnValue + ""));
//                } else if (((String) r.getValue(i)).contains("MULTIPOLYGON")) {
//                    result.addBinding(tempColumnName, vf.createIRI("http://deg.iit.demokritos.gr/invekos/geometry/" + tempColumnValue + ""));
//                } else if (((String) r.getValue(i)).contains("POLYGON")) {
//                    result.addBinding(tempColumnName, vf.createIRI("http://rdf.semagrow.org/pgm/" + dbname + "/resource/" + tempColumnValue + ""));
//                }
//                tempColumnName = tempColumnValue = null;
//            }
////						logger.debug("columnClassName:: {} ", rsmd.getColumnClassName(i+1));
////						logger.debug("columnLabel:: {} ", rsmd.getColumnLabel(i+1));
////						logger.debug("SchemaName:: {} ", rsmd.getSchemaName(i+1));
////						logger.debug("TableName:: {} ", rsmd.getTableName(i+1));
////						logger.debug("CatalogName:: {} ", rsmd.getCatalogName(i+1));
//            logger.debug("columnName:: {} ", rsmd.getColumnName(i+1));
//            logger.debug("columnValue:: {} ", r.getValue(i));
//            
//            if (rsmd.getColumnClassName(i+1).equals("java.lang.String")) {
//            	String geom = "\"" + (String) r.getValue(i) + "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
//            	result.addBinding(rsmd.getColumnName(i+1), vf.createLiteral(geom));
//            }
//            else if (rsmd.getColumnClassName(i+1).equals("java.lang.Double")) {
//            	String dist = "\"" + (Double) r.getValue(i) + "\"^^<http://www.w3.org/2001/XMLSchema#double>";
//            	result.addBinding(rsmd.getColumnName(i+1), vf.createLiteral(dist));
//            }
//            else {
//            	logger.error("java.lang.ClassCastException: {} is {}", rsmd.getColumnName(i+1), rsmd.getColumnClassName(i+1));
//            	throw new SQLException();
//            }
//            logger.debug(" {} as {} ", r.getValue(i), rsmd.getColumnName(i+1));
//
//        }
//        return result;
//    }
	
    /**
     * Merge two bindingSet into one. If some bindings of the second set refer to
     * variable names of the first binding then prefer the bindings of the first set.
     * @param first
     * @param second
     * @return A binding set that contains the union of the variable bindings of first and second set.
     */
    public BindingSet merge(BindingSet first, BindingSet second) {
        QueryBindingSet result = new QueryBindingSet();

        for (Binding b : first) {
            if (!result.hasBinding(b.getName()))
                result.addBinding(b);
        }

        for (String name : second.getBindingNames()) {
            Binding b = second.getBinding(name);
            if (!result.hasBinding(name)) {
                result.addBinding(b);
            }
        }
        return result;
    }

    /**
     * Project a binding set to a potentially smaller binding set that
     * contain only the variable bindings that are in vars set.
     * @param bindings
     * @param vars
     * @return
     */
    public BindingSet project(Collection<String> vars, BindingSet bindings) {
        QueryBindingSet q = new QueryBindingSet();

        for (String varName : vars) {
            Binding b = bindings.getBinding(varName);
            if (b != null) {
                q.addBinding(b);
            }
        }
        return q;
    }


    public BindingSet project(Collection<String> vars, BindingSet bindings, BindingSet parent) {
        return null;
    }


    public Collection<String> projectNames(Collection<String> vars, BindingSet bindings) {
        return null;
    }


    public boolean hasBNode(BindingSet bindings) {
        return false;
    }


    public boolean agreesOn(BindingSet first, BindingSet second) {
        return false;
    }

}
