package eu.semagrow.commons.voidinfer.VOID;

import eu.semagrow.commons.vocabulary.VOID;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iterations;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.InferencerConnection;
import org.openrdf.sail.inferencer.InferencerConnectionWrapper;

import java.util.Iterator;

/**
 * Created by angel on 4/30/14.
 */
public class VOIDInferencerConnection extends InferencerConnectionWrapper
    implements SailConnectionListener {

    private int totalInferred = 0;

    private Model newStatements;
    private Model currentIteration;

    private boolean statementsRemoved = false;

    public VOIDInferencerConnection(InferencerConnection con) {
        super(con);
        con.addConnectionListener(this);
    }

    public void statementAdded(Statement statement) {
        if (statementsRemoved) {
            // No need to record, starting from scratch anyway
            return;
        }

        if (newStatements == null) {
            newStatements = new TreeModel();
        }
        newStatements.add(statement);
    }

    public void statementRemoved(Statement statement) {
        statementsRemoved = true;
        newStatements = null;
    }

    @Override
    public void flushUpdates()
            throws SailException
    {
        super.flushUpdates();

        if (statementsRemoved) {
            //logger.debug("statements removed, starting inferencing from scratch");
            clearInferred();
            addAxiomStatements();

            newStatements = new TreeModel();
            Iterations.addAll(getWrappedConnection().getStatements(null, null, null, true), newStatements);

            statementsRemoved = false;
        }

        doInferencing();
    }

    @Override
    public void rollback()
            throws SailException
    {
        super.rollback();

        statementsRemoved = false;
        newStatements = null;
    }

    protected void addAxiomStatements() throws SailException {

        addInferredStatement(VOID.DATASET, RDF.TYPE, RDFS.CLASS);
        addInferredStatement(VOID.LINKSET, RDFS.SUBCLASSOF, VOID.DATASET);

        addInferredStatement(VOID.SUBSET, RDF.TYPE, RDF.PROPERTY);
        addInferredStatement(VOID.SUBSET, RDFS.DOMAIN, VOID.DATASET);
        addInferredStatement(VOID.SUBSET, RDFS.RANGE, VOID.DATASET);
        addInferredStatement(VOID.PROPERTYPARTITION, RDFS.SUBPROPERTYOF, VOID.SUBSET);
        addInferredStatement(VOID.CLASSPARTITION, RDFS.SUBPROPERTYOF, VOID.SUBSET);

        addInferredStatement(VOID.SPARQLENDPOINT, RDFS.DOMAIN, VOID.DATASET);
        addInferredStatement(VOID.TRIPLES, RDFS.DOMAIN, VOID.DATASET);
        addInferredStatement(VOID.ENTITIES, RDFS.DOMAIN, VOID.DATASET);
        addInferredStatement(VOID.CLASSES, RDFS.DOMAIN, VOID.DATASET);
        addInferredStatement(VOID.DISTINCTOBJECTS, RDFS.DOMAIN, VOID.DATASET);
        addInferredStatement(VOID.DISTINCTSUBJECTS, RDFS.DOMAIN, VOID.DATASET);

        addInferredStatement(VOID.URIREGEXPATTERN, RDFS.DOMAIN, VOID.DATASET);
        addInferredStatement(VOID.PROPERTY, RDFS.DOMAIN, VOID.DATASET);
        addInferredStatement(VOID.CLASS, RDFS.RANGE, RDF.PROPERTY);
    }

    private void prepareIteration() {
        currentIteration = newStatements;
        newStatements = new TreeModel();
    }

    protected void doInferencing()
            throws SailException
    {
        if (!hasNewStatements()) {
            // There's nothing to do
            return;
        }

        // initialize some vars
        totalInferred = 0;
        int iteration = 0;
        int nofInferred = 1;

        // All rules need to be checked:
        //for (int i = 0; i < RDFSRules.RULECOUNT; i++) {
        //    ruleCount[i] = 0;
        //    checkRuleNextIter[i] = true;
        //}

        while (hasNewStatements()) {
            iteration++;
            // logger.debug("starting iteration " + iteration);
            prepareIteration();

            nofInferred = 0;
            nofInferred += applyRule1();
            nofInferred += applyRule2();
            //nofInferred += applyRule(RDFSRules.Rdf1);

            // logger.debug("iteration " + iteration + " done; inferred " + nofInferred + " new statements");
            totalInferred += nofInferred;
        }

        // Print some statistics
        // logger.debug("---RdfMTInferencer statistics:---");
        // logger.debug("total statements inferred = " + totalInferred);
        // for (int i = 0; i < RDFSRules.RULECOUNT; i++) {
        //    logger.debug("rule " + RDFSRules.RULENAMES[i] + ":\t#inferred=" + ruleCount[i]);
        //}
        // logger.debug("---end of statistics:---");
    }

    // xxx void:subset yyy && yyy void:subset zzz --> xxx void:subset zzz
    protected int applyRule1() throws SailException {

        int nofInferred = 0;
        Iterator<Statement> iter = currentIteration.filter(null, VOID.SUBSET, null).iterator();

        while ( iter.hasNext()) {
            Statement st = iter.next();

            Resource yyy1 = st.getSubject();
            Resource zzz1  =  (Resource) st.getObject();
            CloseableIteration<? extends Statement, SailException> t1Iter;
            t1Iter = getWrappedConnection().getStatements(null, VOID.SUBSET, yyy1, true);

            while (t1Iter.hasNext()) {
                Statement t1 = t1Iter.next();
                Resource xxx1 = t1.getSubject();
                boolean added = addInferredStatement(xxx1, VOID.SUBSET, zzz1);
                if (added) {
                    nofInferred++;
                }
            }

            Resource yyy2  =  (Resource) st.getObject();
            t1Iter = getWrappedConnection().getStatements(yyy2, VOID.SUBSET, null, true);

            while (t1Iter.hasNext()) {
                Statement t1 = t1Iter.next();
                Resource xxx2 = t1.getSubject();
                boolean added = addInferredStatement(xxx2, VOID.SUBSET, yyy2);
                if (added) {
                    nofInferred++;
                }
            }
        }

        return nofInferred;
    }


    // xxx void:subset yyy && xxx void:sparqlEndpoint zzz --> yyy void:sparqlEndpoint zzz
    protected int applyRule2() throws SailException {

        int nofInferred = 0;
        Iterator<Statement> iter = currentIteration.filter(null, VOID.SPARQLENDPOINT, null).iterator();

        while ( iter.hasNext()) {
            Statement st = iter.next();

            Resource xxx = st.getSubject();
            Value zzz  = st.getObject();
            CloseableIteration<? extends Statement, SailException> t1Iter;
            t1Iter = getWrappedConnection().getStatements(xxx, VOID.SUBSET, null, true);

            while (t1Iter.hasNext()) {
                Statement t1 = t1Iter.next();
                Resource yyy = (Resource) t1.getObject();
                boolean added = addInferredStatement(yyy, VOID.SPARQLENDPOINT, zzz);
                if (added) {
                    nofInferred++;
                }
            }
        }

        Iterator<Statement> iter1 = currentIteration.filter(null, VOID.SUBSET, null).iterator();

        while ( iter1.hasNext()) {
            Statement st = iter1.next();

            Resource xxx = st.getSubject();
            Resource yyy  = (Resource) st.getObject();
            CloseableIteration<? extends Statement, SailException> t1Iter;
            t1Iter = getWrappedConnection().getStatements(xxx, VOID.SPARQLENDPOINT, null, true);

            while (t1Iter.hasNext()) {
                Statement t1 = t1Iter.next();
                Value zzz =  t1.getObject();
                boolean added = addInferredStatement(yyy, VOID.SPARQLENDPOINT, zzz);
                if (added) {
                    nofInferred++;
                }
            }
        }


        return nofInferred;
    }

    protected boolean hasNewStatements() {
        return newStatements != null && !newStatements.isEmpty();
    }

}
