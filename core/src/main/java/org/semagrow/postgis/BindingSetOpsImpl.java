package org.semagrow.postgis;

import org.jooq.Record;
import org.semagrow.evaluation.BindingSetOps;
import org.semagrow.evaluation.reactor.FederatedEvaluationStrategyImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public final class BindingSetOpsImpl implements BindingSetOps {

	private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);
	private static final ValueFactory vf = SimpleValueFactory.getInstance();

    public static final BindingSet transform(Record r, List<String> tables) throws SQLException {

        ResultSet rs = r.intoResultSet();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        QueryBindingSet result = new QueryBindingSet();
        String tempColumnName = null, tempColumnValue = null;
        logger.info("columnsNumber::::::::::::::::::::::: {} ", columnsNumber);
        logger.info("tables::::::::::::::::::::::: {} ", tables.toString());
        for (int i = 1; i <= columnsNumber; i++) {
            if (rsmd.getColumnClassName(i).equals("java.lang.Integer")) {
                logger.info("string is numeric!!!: {} ", rsmd.getColumnName(i));
                logger.info("tables[{}]: {} ", i - 1, tables.get(i-1));
                if (tables.get(i - 1).equals("?")) {
                    tempColumnName = rsmd.getColumnName(i);
                    tempColumnValue = r.getValue(i-1) instanceof String ? (String) r.getValue(i-1) : r.getValue(i-1).toString();
                } else {
                    result.addBinding(
                            rsmd.getColumnName(i),
                            vf.createIRI("http://deg.iit.demokritos.gr/" + tables.get(i-1) + "/resource/Geometry/" + r.getValue(i-1) + "")
                    );
                }
                continue;
            }
            if (tempColumnName != null && tempColumnValue != null) {
                logger.info("tables[{}]: {} ", i - 1, tables.get(i - 1));
                if (((String) r.getValue(i-1)).contains("POINT")) {
                    result.addBinding(tempColumnName, vf.createIRI("http://deg.iit.demokritos.gr/lucas/resource/Geometry/" + tempColumnValue + ""));
                } else if (((String) r.getValue(i-1)).contains("MULTIPOLYGON")) {
                    result.addBinding(tempColumnName, vf.createIRI("http://deg.iit.demokritos.gr/invekos/resource/Geometry/" + tempColumnValue + ""));
                }
                tempColumnName = tempColumnValue = null;
            }
//						logger.info("columnClassName:: {} ", rsmd.getColumnClassName(i));
//						logger.info("columnLabel:: {} ", rsmd.getColumnLabel(i));
//						logger.info("SchemaName:: {} ", rsmd.getSchemaName(i));
//						logger.info("TableName:: {} ", rsmd.getTableName(i));
//						logger.info("CatalogName:: {} ", rsmd.getCatalogName(i));
            logger.info("columnName:: {} ", rsmd.getColumnName(i));
            logger.info("columnValue:: {} ", r.getValue(i-1));
            result.addBinding(rsmd.getColumnName(i), vf.createLiteral((String) r.getValue(i-1)));
            logger.info(" {} as {} ", r.getValue(i-1), rsmd.getColumnName(i));

        }
        return result;
    }
	
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
