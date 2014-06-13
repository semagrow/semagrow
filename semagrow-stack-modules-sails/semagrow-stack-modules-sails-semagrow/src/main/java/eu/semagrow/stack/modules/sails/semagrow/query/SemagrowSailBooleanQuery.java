package eu.semagrow.stack.modules.sails.semagrow.query;

import eu.semagrow.stack.modules.api.query.SemagrowBooleanQuery;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSailRepositoryConnection;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.repository.sail.SailBooleanQuery;

/**
 * Created by angel on 6/13/14.
 */
public class SemagrowSailBooleanQuery extends SailBooleanQuery
        implements SemagrowBooleanQuery {

    public SemagrowSailBooleanQuery(ParsedBooleanQuery tupleQuery, SemagrowSailRepositoryConnection sailConnection) {
        super(tupleQuery, sailConnection);
    }
}
