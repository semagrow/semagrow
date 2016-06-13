package eu.semagrow.commons;

import org.eclipse.rdf4j.model.Literal;

/**
 * Plain Literal is an ordinary Literal that is created with his type infered.
 * This interface is used to tag the Literals that have no explicit type from the user
 * and the XMLSchema.STRING type is automatically inferred.
 */
public interface PlainLiteral extends Literal {

}
