//package org.semagrow.connector.postgis.execution;
//
//import java.util.Collections;
//import java.util.List;
//
//import org.eclipse.rdf4j.model.IRI;
//import org.eclipse.rdf4j.query.BindingSet;
//import org.eclipse.rdf4j.query.algebra.StatementPattern;
//import org.eclipse.rdf4j.query.algebra.TupleExpr;
//import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
//
//public class PostGISQueryTransformer {
//
//	private String base;
//	private IRI endpoint;
//	
//    public String transformQuery(String base, IRI endpoint, TupleExpr expr) {
//    	return transformQuery(base, endpoint, expr, Collections.emptyList());
//    }
//    
//    public String transformQuery(String base, IRI endpoint, TupleExpr expr, List<BindingSet> bindingSetList) {
//    	this.endpoint = endpoint;
//        this.base = base;
//        String table, gid;
//        
//        List<StatementPattern> statementPatterns = StatementPatternCollector.process(expr);
//        
//        table = "lucas";
//        gid = "9";
//        StringBuilder stringBuilder = new StringBuilder();
//        stringBuilder.append("SELECT ST_AsText(geom) FROM ");
//        stringBuilder.append(table);
//        stringBuilder.append(" WHERE gid=");
//        stringBuilder.append(gid);
//        stringBuilder.append(";");
//
//        return stringBuilder.toString();
//    }
//    
//}
