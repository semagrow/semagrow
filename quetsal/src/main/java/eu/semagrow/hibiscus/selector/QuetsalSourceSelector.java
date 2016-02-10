package eu.semagrow.hibiscus.selector;

import com.fluidops.fedx.EndpointManager;
import com.fluidops.fedx.FedX;
import com.fluidops.fedx.FederationManager;
import com.fluidops.fedx.algebra.StatementSource;
import com.fluidops.fedx.cache.Cache;
import com.fluidops.fedx.structures.Endpoint;
import eu.semagrow.core.source.SourceMetadata;
import eu.semagrow.hibiscus.util.BPGCollector;
import org.aksw.sparql.query.algebra.helpers.BasicGraphPatternExtractor;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;

import java.util.*;

/**
 * Created by angel on 26/6/2015.
 */
public class QuetsalSourceSelector {

    protected Cache cache;
    protected List<Endpoint> members;

    public QuetsalSourceSelector() {

        cache = FederationManager.getInstance().getCache();
        FedX fed = FederationManager.getInstance().getFederation();
        members = fed.getMembers();
    }

    protected List<SourceMetadata> toSourceMetadata(Map<StatementPattern, List<StatementSource>> lst)
    {
        List<SourceMetadata> metadata = new LinkedList<>();

        for (StatementPattern pattern : lst.keySet()) {
            List<StatementSource> sources = lst.get(pattern);
            if (!sources.isEmpty()) {
                for (StatementSource src : sources) {
                    URI endpoint = toURI(src);
                    metadata.add(new SourceMetadata() {
                        @Override
                        public List<URI> getEndpoints() {
                            return Collections.singletonList(endpoint);
                        }

                        @Override
                        public StatementPattern original() {
                            return pattern;
                        }

                        @Override
                        public StatementPattern target() {
                            return pattern;
                        }

                        @Override
                        public Collection<URI> getSchema(String var) {
                            return null;
                        }

                        @Override
                        public boolean isTransformed() {
                            return false;
                        }

                        @Override
                        public double getSemanticProximity() {
                            return 0;
                        }
                    });
                }
            }
        }
        return metadata;
    }

    private URI toURI(StatementSource src)
    {
        String endpointId = src.getEndpointID();

        return ValueFactoryImpl.getInstance().createURI(
                EndpointManager.getEndpointManager().getEndpoint(endpointId).getEndpoint() );
    }

    protected HashMap<Integer, List<StatementPattern>> generateBgpGroups(TupleExpr expr) {

        HashMap<Integer, List<StatementPattern>> bgpGrps = new HashMap<Integer, List<StatementPattern>>();
        int grpNo = 0;

        TupleExpr e = expr.clone();

        List<TupleExpr> bgps = BasicGraphPatternExtractor.process(e);

        if (bgps.isEmpty())
            bgps = Collections.singletonList(new QueryRoot(e));

        for (TupleExpr bgp : bgps) {
            List<StatementPattern> patterns = StatementPatternCollector.process(bgp);
            bgpGrps.put(grpNo, patterns );
            grpNo++;
        }
        return bgpGrps;
    }
}
