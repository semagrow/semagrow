package eu.semagrow.hibiscus.selector;

import eu.semagrow.core.source.SourceMetadata;
import eu.semagrow.core.source.SourceSelector;
import org.aksw.simba.quetzal.core.HibiscusSourceSelection;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryException;

import java.util.*;

/**
 * Created by angel on 15/6/2015.
 */
public class HibiscusSourceSelector extends QuetsalSourceSelector implements SourceSelector {

    private HibiscusSourceSelection impl;

    public HibiscusSourceSelector() { super(); }

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
            this.impl = new HibiscusSourceSelection(members, cache, query);
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
