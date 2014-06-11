package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.sails.semagrow.algebra.SingleSourceExpr;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Service;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;

/**
 * Used to translate SingleSourceExpr expressions to Service expressions.
 * Execute this optimizer in order to produce a tupleExpr that can be
 * evaluated by the vanilla sesame engine.
 * @author acharal@iit.demokritos.gr
 */
@Deprecated
public class SingleSourcetoServiceConverter implements QueryOptimizer {

    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new SingleSourceVisitor());
    }

    protected static class SingleSourceVisitor extends QueryModelVisitorBase<RuntimeException> {

        @Override
        public void meetNode(QueryModelNode node) {
            if (node instanceof SingleSourceExpr) {
                SingleSourceExpr expr = (SingleSourceExpr)node;
                Var serviceRef = new Var("service", ValueFactoryImpl.getInstance().createURI(expr.getSource().toString()));

                try {
                    TupleExpr e = expr.getArg();
                    String query = tupleExprToString(e);
                    Service service = new Service(serviceRef, e, query, null, null, true);
                    node.replaceWith(service);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            else
                super.meetNode(node);
        }

        @Override
        public void meetOther(QueryModelNode node) {
            meetNode(node);
        }

        private String tupleExprToString(TupleExpr arg) throws Exception {
            SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
            ParsedTupleQuery query = new ParsedTupleQuery(arg);
            return renderer.render(query);
        }
    }
}
