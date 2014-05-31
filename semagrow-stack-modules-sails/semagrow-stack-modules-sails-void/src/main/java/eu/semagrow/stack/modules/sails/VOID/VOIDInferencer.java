package eu.semagrow.stack.modules.sails.VOID;

import org.openrdf.sail.NotifyingSail;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.NotifyingSailWrapper;
import org.openrdf.sail.inferencer.InferencerConnection;

/**
 * Created by angel on 4/30/14.
 */
public class VOIDInferencer extends NotifyingSailWrapper {


    public VOIDInferencer() { super(); }

    public VOIDInferencer(NotifyingSail sailBase) {
        super(sailBase);
    }


    @Override
    public VOIDInferencerConnection getConnection()
        throws SailException
    {
        try {
            InferencerConnection con = (InferencerConnection)super.getConnection();
            return new VOIDInferencerConnection(con);
        }
        catch (ClassCastException e) {
            throw new SailException(e.getMessage(), e);
        }
    }

    public void initialize()
            throws SailException
    {
        super.initialize();

        VOIDInferencerConnection con = getConnection();
        try {
            con.begin();
            con.addAxiomStatements();
            con.flushUpdates();
            con.commit();
        }
        finally {
            con.close();
        }
    }


}
