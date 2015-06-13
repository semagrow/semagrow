package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * Created by angel on 18/2/2015.
 */
public class VarRenameProcessor {

    protected static class VarRenameVisitor extends QueryModelVisitorBase<RuntimeException> {

        //private Map<String,String> mappings;

        //public VarRenameVisitor(Map<String,String> mappings) { this.mappings = mappings; }

        private String suffix = "";

        public VarRenameVisitor(String suffix) { this.suffix = suffix; }

        @Override
        public void meet(Var var) throws RuntimeException {
            if (!var.hasValue()) {
                var.setName(var.getName() + suffix);
            }
        }
    }

    public static void process(TupleExpr expr, String suffix) {
        VarRenameVisitor renamer = new VarRenameVisitor(suffix);
        expr.visit(renamer);
    }
}
