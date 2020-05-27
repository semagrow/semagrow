//package org.semagrow.postgis;
//
//import org.semagrow.evaluation.BindingSetOps;
//import org.semagrow.evaluation.reactor.FederatedEvaluationStrategyImpl;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.eclipse.rdf4j.model.ValueFactory;
//import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
//import org.eclipse.rdf4j.query.Binding;
//import org.eclipse.rdf4j.query.BindingSet;
//import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
//
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
//import java.util.Collection;
//
//public class BindingSetOpsImpl implements BindingSetOps {
//
//	private static final Logger logger = LoggerFactory.getLogger(FederatedEvaluationStrategyImpl.class);
//	private static final ValueFactory vf = SimpleValueFactory.getInstance();
//	
//	/**
//     * Transform a bindingSet from a resultSet.
//     * @param rs
//     * @return A binding set that contains the results from the result set.
//     */
////	public BindingSet transform(ResultSet rs) {
////        QueryBindingSet result = new QueryBindingSet();
////        logger.info("BindingSetOpsImpl transform!!!!! ");
////        try {
////			ResultSetMetaData rsmd = rs.getMetaData();
////			int columnsNumber = rsmd.getColumnCount();
////			while (rs.next()) {
////				for (int i = 1; i <= columnsNumber; i++) {
////					String columnValue = rs.getString(i);
////					logger.info("columnName:: {} ", rsmd.getColumnName(i));
////					logger.info("columnValue:: {} ", columnValue);
//////					vf.createLiteral(columnValue);
////					result.addBinding(rsmd.getColumnName(i), vf.createLiteral(columnValue));
////					logger.info(" {} as {} ", columnValue, rsmd.getColumnName(i));
////				}
////			}
////		} catch (SQLException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}
////        return result;
////    }
//	
//    /**
//     * Merge two bindingSet into one. If some bindings of the second set refer to
//     * variable names of the first binding then prefer the bindings of the first set.
//     * @param first
//     * @param second
//     * @return A binding set that contains the union of the variable bindings of first and second set.
//     */
//    public BindingSet merge(BindingSet first, BindingSet second) {
//        QueryBindingSet result = new QueryBindingSet();
//
//        for (Binding b : first) {
//            if (!result.hasBinding(b.getName()))
//                result.addBinding(b);
//        }
//
//        for (String name : second.getBindingNames()) {
//            Binding b = second.getBinding(name);
//            if (!result.hasBinding(name)) {
//                result.addBinding(b);
//            }
//        }
//        return result;
//    }
//
//    /**
//     * Project a binding set to a potentially smaller binding set that
//     * contain only the variable bindings that are in vars set.
//     * @param bindings
//     * @param vars
//     * @return
//     */
//    public BindingSet project(Collection<String> vars, BindingSet bindings) {
//        QueryBindingSet q = new QueryBindingSet();
//
//        for (String varName : vars) {
//            Binding b = bindings.getBinding(varName);
//            if (b != null) {
//                q.addBinding(b);
//            }
//        }
//        return q;
//    }
//
//
//    public BindingSet project(Collection<String> vars, BindingSet bindings, BindingSet parent) {
//        return null;
//    }
//
//
//    public Collection<String> projectNames(Collection<String> vars, BindingSet bindings) {
//        return null;
//    }
//
//
//    public boolean hasBNode(BindingSet bindings) {
//        return false;
//    }
//
//
//    public boolean agreesOn(BindingSet first, BindingSet second) {
//        return false;
//    }
//
//}
