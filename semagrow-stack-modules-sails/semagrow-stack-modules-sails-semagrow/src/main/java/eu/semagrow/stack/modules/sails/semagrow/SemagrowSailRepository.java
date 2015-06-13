package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.api.repository.SemagrowRepository;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.base.RepositoryWrapper;
import org.openrdf.repository.sail.SailRepository;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowSailRepository
        extends RepositoryWrapper
        implements SemagrowRepository
{
    private SemagrowSail semagrowSail;

    public SemagrowSailRepository(SemagrowSail sail) {
        super(new SailRepository(sail));
        semagrowSail = sail;
    }

    @Override
    public SemagrowSailRepositoryConnection getConnection() throws RepositoryException {
        return new SemagrowSailRepositoryConnection(this, super.getConnection());
    }

    public Repository getMetadataRepository() { return semagrowSail.getMetadataRepository(); }
}
