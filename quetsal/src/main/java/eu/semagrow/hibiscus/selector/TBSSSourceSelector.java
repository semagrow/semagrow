package eu.semagrow.hibiscus.selector;

import eu.semagrow.core.source.SourceMetadata;
import eu.semagrow.core.source.SourceSelector;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryException;
import org.aksw.simba.quetzal.core.TBSSSourceSelection;

import java.util.*;

/**
 * Created by angel on 24/6/2015.
 */
public class TBSSSourceSelector extends QuetsalSourceSelector implements SourceSelector {

    private TBSSSourceSelection impl;

    public TBSSSourceSelector() { super(); }

    @Override
    public List<SourceMetadata> getSources(StatementPattern pattern, Dataset dataset, BindingSet bindings) {
        return null;
    }

    @Override
    public List<SourceMetadata> getSources(Iterable<StatementPattern> patterns, Dataset dataset, BindingSet bindings) {
        return null;
    }

    @Override
    public List<SourceMetadata> getSources(TupleExpr expr, Dataset dataset, BindingSet bindings)
    {
        String query = null;
        try {
            this.impl = new TBSSSourceSelection(members, cache, query);
            HashMap<Integer, List<StatementPattern>> bgpGrps =  generateBgpGroups(expr);
            return toSourceMetadata(this.impl.performSourceSelection(bgpGrps));
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        }

        return null;
    }

}
