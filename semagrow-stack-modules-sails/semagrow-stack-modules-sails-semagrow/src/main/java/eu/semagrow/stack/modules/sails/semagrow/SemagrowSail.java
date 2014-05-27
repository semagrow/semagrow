package eu.semagrow.stack.modules.sails.semagrow;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;

import java.io.File;

/**
 * Semagrow Sail implementation.
 * @author acharal@iit.demokritos.gr
 *
 * TODO list and other suggestions from the plenary meeting in Wageningen
 * TODO: lineage of evaluation (track the sources of the produced tuples)
 * TODO: define clean interfaces for sourceselector
 * TODO: rethink voID descriptions
 * TODO: estimate processing cost of subqueries to the sources (some sources may contain indexes etc
 * TODO: order-by and sort-merge-join
 * TODO: voID and configuration as sailbase and able to be SPARQL queried.
 * TODO: do transformation
 * TODO: geosparql
 */
public class SemagrowSail extends SailBase {


    @Override
    protected void shutDownInternal() throws SailException {

    }

    public boolean isWritable() throws SailException {
        return false;
    }

    /**
     * Creates a new Semagrow SailConnection
     * @return a new SailConnection
     * @throws SailException
     */
    @Override
    public SailConnection getConnectionInternal() throws SailException {
        return new SemagrowConnection(this);
    }

    public ValueFactory getValueFactory() {
        return ValueFactoryImpl.getInstance();
    }
}
