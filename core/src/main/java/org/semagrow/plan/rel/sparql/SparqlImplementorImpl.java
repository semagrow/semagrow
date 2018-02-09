package org.semagrow.plan.rel.sparql;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.queryrender.QueryRenderer;
import org.semagrow.plan.rel.RelToTupleExprConverter;

public class SparqlImplementorImpl
        extends RelToTupleExprConverter
        implements SparqlImplementor {

    @Override
    public Result implement(RelNode node) {
        TupleExpr root = dispatch(node);
        return result(root);
    }

    public TupleExpr visit(SparqlStatementPattern pattern) {
        return null;
    }


    @Override public TupleExpr visit(RelNode rel) {
        if (rel instanceof SparqlRel)
            return ((SparqlRel)rel).implement(this).asTupleExpr();
        else
            return super.visit(rel);
    }


    protected Result result(TupleExpr e) {
        return new ResultImpl(e);
    }

    class ResultImpl implements Result {

        private TupleExpr root;

        ResultImpl(TupleExpr root) {
            this.root = Preconditions.checkNotNull(root);
        }


        @Override
        public String asQueryString() {
            final QueryRenderer renderer = new SparqlQueryRenderer();
            try {
                return renderer.render(new ParsedTupleQuery(root));
            } catch (Exception e) {
                throw new AssertionError();
            }
        }

        public TupleExpr asTupleExpr() {
            return root;
        }
    }

}
