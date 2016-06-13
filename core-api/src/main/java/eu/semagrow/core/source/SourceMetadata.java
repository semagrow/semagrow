package eu.semagrow.core.source;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

import java.util.Collection;
import java.util.List;

/**
 * Source Metadata
 * 
 * Interface for all components that serve metadata about datasets.
 * Contains only info about the source and the required transformations.
 * The term SourceMetadata does not refer only to a certain endpoint.
 * Supports alternative endpoints for the same source (e.g. mirrored)
 * Each endpoint contain exactly the same triples, so it is considered as the same source.
 * Endpoints that contain different number of triples are considered different sources.
 * 
 * @author Angelos Charalambidis
 */

public interface SourceMetadata {

    /**
     * Gets alternative endpoints for the same dataset
     * @return
     */
    List<Site> getSites();

    /**
     * Gets the pattern which the datasource contain triples
     * @return
     */
    StatementPattern original();

    StatementPattern target();

    Collection<IRI> getSchema(String var);

    /**
     *
     * @return {@code true} if the pattern must be transformed
     */
    // TODO: must return the kind of transformation (target vocabulary)
    boolean isTransformed();

    /**
     * Returns an estimation of how close are the transformed results to the initial pattern
     * @return
     */
    double getSemanticProximity();

}
