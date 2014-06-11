package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.api.repository.SemagrowRepository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;

/**
 * Created by angel on 6/10/14.
 */
public class SemagrowSailRepository extends SailRepository implements SemagrowRepository {

    private SemagrowSail semagrowSail;

    public SemagrowSailRepository(SemagrowSail sail) {
        super(sail);
        semagrowSail = sail;
    }

    public SemagrowSailRepositoryConnection getConnection() throws RepositoryException {
        try {
            return new SemagrowSailRepositoryConnection(this, (SemagrowSailConnection) semagrowSail.getConnection());
        } catch(Exception e) {
            throw new RepositoryException(e);
        }
    }
}
