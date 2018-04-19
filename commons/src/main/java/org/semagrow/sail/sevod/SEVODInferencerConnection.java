package org.semagrow.sail.sevod;

import org.semagrow.sail.VOID.VOIDInferencerConnection;
import org.semagrow.model.vocabulary.SEVOD;
import org.semagrow.model.vocabulary.VOID;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.inferencer.InferencerConnection;

/**
 * Created by angel on 7/4/14.
 */
public class SEVODInferencerConnection extends VOIDInferencerConnection {

    public SEVODInferencerConnection(InferencerConnection con) {
        super(con);
    }

    @Override
    protected void addAxiomStatements() throws SailException {
        super.addAxiomStatements();
        addInferredStatement(SEVOD.SUBJECTCLASS, RDFS.SUBPROPERTYOF, VOID.CLASS);
        addInferredStatement(SEVOD.OBJECTCLASS, RDFS.SUBPROPERTYOF, VOID.CLASS);
        addInferredStatement(SEVOD.SUBJECTREGEXPATTERN, RDFS.SUBPROPERTYOF, VOID.URIREGEXPATTERN);
        addInferredStatement(SEVOD.OBJECTREGEXPATTERN, RDFS.SUBPROPERTYOF, VOID.URIREGEXPATTERN);
    }
}
