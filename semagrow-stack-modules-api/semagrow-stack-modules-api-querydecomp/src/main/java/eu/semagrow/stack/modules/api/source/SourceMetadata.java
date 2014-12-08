package eu.semagrow.stack.modules.api.source;

import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;

import java.util.Collection;
import java.util.List;

/**
 * Partially the interface of SelectedResource.
 * Contains only info about the source and the required transformations.
 * The term SourceMetadata does not refer only to a certain endpoint.
 * Supports alternative endpoints for the same source (e.g. mirrored)
 * Each endpoint contain exactly the same triples, so it is considered as the same source.
 * Endpoints that contain different number of triples are considered different sources.
 * Created by angel on 5/22/14.
 */
public interface SourceMetadata {

    /**
     * Gets alternative endpoints for the same dataset
     * @return
     */
    List<URI> getEndpoints();

    /**
     * Gets the pattern which the datasource contain triples
     * @return
     */
    StatementPattern original();

    StatementPattern target();

    Collection<URI> getSchema(String var);

    /**
     *
     * @return true if the pattern must be transformed
     * TODO: must return the kind of transformation (target vocabulary)
     */
    boolean isTransformed();

    /**
     * Returns an estimation of how close are the transformed results to the initial pattern
     * @return
     */
    double getSemanticProximity();

}
