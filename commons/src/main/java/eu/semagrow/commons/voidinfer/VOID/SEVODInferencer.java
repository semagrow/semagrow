package eu.semagrow.commons.voidinfer.VOID;

import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.inferencer.InferencerConnection;

/**
 * Created by angel on 7/4/14.
 */
public class SEVODInferencer extends VOIDInferencer {

    @Override
    public VOIDInferencerConnection getConnection()
            throws SailException
    {
        try {
            VOIDInferencerConnection conn = super.getConnection();
            InferencerConnection con = conn.getWrappedConnection();
            return new SEVODInferencerConnection(con);
        }
        catch (ClassCastException e) {
            throw new SailException(e.getMessage(), e);
        }
    }
}
