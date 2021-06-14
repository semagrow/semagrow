package org.semagrow.connector.postgis.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.jooq.Record;
import org.semagrow.evaluation.BindingSetOps;

public final class BindingSetOpsImpl implements BindingSetOps {

	private static final ValueFactory vf = SimpleValueFactory.getInstance();

	private static BindingSetOpsImpl defaultImpl;

    private BindingSetOpsImpl() { }

    public static BindingSetOps getInstance() {
        if (defaultImpl == null)
            defaultImpl = new BindingSetOpsImpl();
        return defaultImpl;
    }
	
    public static final BindingSet transform(Record r, String dbname, Map<String,String> extraBindingVars) 
    		throws SQLException {

        ResultSet rs = r.intoResultSet();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        QueryBindingSet result = new QueryBindingSet();
        for (int i = 0; i < columnsNumber; i++) {
            if (rsmd.getColumnClassName(i+1).equals("java.lang.Integer")) {
                String id = "http://rdf.semagrow.org/pgm/" + dbname + "/resource/" + r.getValue(i);
                result.addBinding(rsmd.getColumnName(i+1), vf.createIRI(id));
            }
            else if (rsmd.getColumnClassName(i+1).equals("java.lang.String")) {
            	/* <http://www.opengis.net/ont/geosparql#wktLiteral> */
            	result.addBinding(rsmd.getColumnName(i+1), vf.createLiteral((String) r.getValue(i), GEO.WKT_LITERAL));
            }
            else if (rsmd.getColumnClassName(i+1).equals("java.lang.Double")) {
            	/* <http://www.w3.org/2001/XMLSchema#double> */
            	result.addBinding(rsmd.getColumnName(i+1), vf.createLiteral((Double) r.getValue(i)));
            }
            else {
            	throw new SQLException();
            }
        }
        
        for (Map.Entry<String,String> binding : extraBindingVars.entrySet()) {
        	result.addBinding(binding.getKey(), vf.createIRI(binding.getValue()));
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
    @Override
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
    @Override
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

    @Override
    public BindingSet project(Collection<String> vars, BindingSet bindings, BindingSet parent) {
    	BindingSet result = merge(bindings, parent);// new QueryBindingSet(parent);
        return project(vars, result);
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