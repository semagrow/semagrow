package eu.semagrow.commons.voidinfer.VOID;

import eu.semagrow.commons.vocabulary.SEVOD;
import eu.semagrow.commons.vocabulary.VOID;
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
