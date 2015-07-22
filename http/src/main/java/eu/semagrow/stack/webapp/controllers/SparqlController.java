package eu.semagrow.stack.webapp.controllers;

import eu.semagrow.commons.utils.FileUtils;
import eu.semagrow.core.decomposer.QueryDecompositionException;
import eu.semagrow.query.SemagrowTupleQuery;
import eu.semagrow.repository.SemagrowRepository;
import eu.semagrow.commons.CONSTANTS;

import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import eu.semagrow.config.SemagrowRepositoryConfig;
import eu.semagrow.query.impl.SemagrowSailTupleQuery;
import eu.semagrow.stack.webapp.controllers.exceptions.SemaGrowBadRequestException;
import eu.semagrow.stack.webapp.controllers.exceptions.SemaGrowException;
import eu.semagrow.stack.webapp.controllers.exceptions.SemaGrowExternalError;
import eu.semagrow.stack.webapp.controllers.exceptions.SemaGrowInternalException;
import eu.semagrow.stack.webapp.controllers.exceptions.SemaGrowNotAcceptableException;
import eu.semagrow.stack.webapp.controllers.exceptions.SemaGrowTimeOutException;
import eu.semagrow.utils.queryresultio.HTMLTableWriter;
import eu.semagrow.utils.sparqlutils.SparqlUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.*;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Graph;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.query.*;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.parser.ParsedOperation;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedUpdate;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.query.resultio.BooleanQueryResultFormat;
import org.openrdf.query.resultio.BooleanQueryResultParserRegistry;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParserRegistry;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.openrdf.query.resultio.sparqlxml.SPARQLResultsXMLWriter;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.*;
import org.openrdf.repository.http.HTTPQueryEvaluationException;
import org.openrdf.repository.sail.config.SailRepositoryConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.rio.n3.N3Writer;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;
import org.openrdf.rio.trig.TriGWriter;
import org.openrdf.rio.trix.TriXWriter;
import org.openrdf.rio.turtle.TurtleWriter;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.rdbms.exceptions.RdbmsQueryEvaluationException;
import org.openrdf.sail.rdbms.exceptions.UnsupportedRdbmsOperatorException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
@Controller
@RequestMapping("/sparql")
public class SparqlController {
    private final Logger  LOG = LoggerFactory.getLogger(SparqlController.class);
    private SemagrowRepository repository;
    
    public SparqlController() throws RepositoryException {            
    }
    
    @PostConstruct
    public void startUp() throws RepositoryException, RepositoryConfigException {
        SemagrowRepositoryConfig repoConfig = getConfig();
        RepositoryFactory repoFactory = RepositoryRegistry.getInstance().get(repoConfig.getType());
        repository = (SemagrowRepository) repoFactory.getRepository(repoConfig);
        repository.initialize();

        // remove CSV and TSV format due to bug: literals are recognized as URIs if they contain a substring parsable as URI.
        TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry.getInstance();
        registry.remove(registry.get(TupleQueryResultFormat.CSV));
        registry.remove(registry.get(TupleQueryResultFormat.TSV));
        registry.remove(registry.get(TupleQueryResultFormat.JSON));

        BooleanQueryResultParserRegistry booleanRegistry = BooleanQueryResultParserRegistry.getInstance();
        booleanRegistry.remove(booleanRegistry.get(BooleanQueryResultFormat.JSON));
    }
    
    @PreDestroy
    public void shutDown(){
        try {
            this.repository.shutDown();
        } catch (RepositoryException ex) {}
    }

    private SemagrowRepositoryConfig getConfig() {

        try {
            File file = FileUtils.getFile("repository.ttl");
            Graph configGraph = parseConfig(file);
            RepositoryConfig repConf = RepositoryConfig.create(configGraph, null);
            repConf.validate();
            RepositoryImplConfig implConf = repConf.getRepositoryImplConfig();
            return (SemagrowRepositoryConfig)implConf;
        } catch (RepositoryConfigException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        } catch (SailConfigException | IOException | NullPointerException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        }
    }

    protected Graph parseConfig(File file) throws SailConfigException, IOException {

        RDFFormat format = Rio.getParserFormatForFileName(file.getAbsolutePath());
        if (format==null)
            throw new SailConfigException("Unsupported file format: " + file.getAbsolutePath());
        RDFParser parser = Rio.createParser(format);
        Graph model = new GraphImpl();
        parser.setRDFHandler(new StatementCollector(model));
        InputStream stream = new FileInputStream(file);

        try {
            parser.parse(stream, file.getAbsolutePath());
        } catch (Exception e) {
            throw new SailConfigException("Error parsing file!");
        }

        stream.close();
        return model;
    }

    @RequestMapping(method=RequestMethod.GET)
    public ModelAndView getSparqlEndpointForm(HttpServletResponse response) throws IOException{
        ModelAndView mav = new ModelAndView("sparqlendpoint");
        return mav;
    }
    
    @RequestMapping(value="", params={ CONSTANTS.WEBAPP.PARAM_QUERY })
    public void query(HttpServletResponse response, HttpServletRequest request, 
            @RequestParam String query, 
            @RequestParam(defaultValue="") String prefixes,
            @RequestParam(value=CONSTANTS.WEBAPP.PARAM_ACCEPT, defaultValue="", required=false) String accept) 
            throws MalformedQueryException, RepositoryException, UpdateExecutionException, IOException, QueryEvaluationException, TupleQueryResultHandlerException, RDFHandlerException, SemaGrowTimeOutException, SemaGrowInternalException, SemaGrowExternalError, SemaGrowBadRequestException, Throwable {
        
        if(!prefixes.trim().equals("")){
            query = prefixes.concat("\n").concat(query);
        }
        
        RepositoryConnection repCon = null;
        
        try {
            ParsedOperation pO = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, query, null);
            if(pO instanceof ParsedUpdate){
                if(request.getUserPrincipal()!=null && request.isUserInRole(CONSTANTS.WEBAPP.ROLES.ROLE_SEMAGROW_ADMIN)){
                    repCon = this.repository.getConnection();                    
                    repCon.begin();
                    repCon.prepareUpdate(QueryLanguage.SPARQL, query).execute();                    
                    repCon.commit();                    
                    response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                } else {
                    if(request.getUserPrincipal()==null){
                        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                    } else {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    }
                }
            }
            if(pO instanceof ParsedQuery){
                repCon = this.repository.getConnection();
                Query q = repCon.prepareQuery(QueryLanguage.SPARQL, query);
                String acceptFormat = !accept.equals("")?accept:request.getHeader("accept");
                accept = SparqlUtils.getAcceptMimeType(q, acceptFormat);
                response.setContentType(accept);

                if (q instanceof SemagrowSailTupleQuery) {
                    SemagrowSailTupleQuery qq = (SemagrowSailTupleQuery)q;
                    Dataset activeDataset = qq.getActiveDataset();
                    if (activeDataset != null) {
                    	Set<URI> namedGraphs = qq.getActiveDataset().getNamedGraphs();
                        // use named graph as include only sources
                        if (namedGraphs != null && !namedGraphs.isEmpty()) {
                            for (URI u  : namedGraphs) {
                                qq.addIncludedSource(u);
                            }
                        }
                    }
                }

                this.handleQuery(response.getOutputStream(), accept, q);
            }            
        } finally {
            if(repCon!=null){
                repCon.close();
            }
        }
    }
            
    @RequestMapping(value="/explain", method=RequestMethod.POST, params={ CONSTANTS.WEBAPP.PARAM_QUERY })
    public void explain(HttpServletResponse response, HttpServletRequest request, @RequestParam String query, @RequestParam(defaultValue="") String prefixes) 
            throws IOException, RepositoryException, RepositoryException, MalformedQueryException {
        if(!prefixes.trim().equals("")){
            query = prefixes.concat("\n").concat(query);
        }
        
        response.setContentType("text/plain");
        RepositoryConnection repCon = null;
        try {
            repCon = this.repository.getConnection();
            ParsedOperation pO = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, query, null);
            if(pO instanceof ParsedUpdate){
                response.getWriter().append(repCon.prepareUpdate(QueryLanguage.SPARQL, query).toString());
            }
            if(pO instanceof ParsedQuery){
                response.getWriter().append(repCon.prepareQuery(QueryLanguage.SPARQL, query).toString());
            }            
            response.getWriter().flush();
        } finally {
            if(repCon!=null){
                repCon.close();
            }
        }
    }


    @RequestMapping(value="/decompose", method=RequestMethod.POST, params={ CONSTANTS.WEBAPP.PARAM_QUERY })
    public void decompose(HttpServletResponse response, HttpServletRequest request, @RequestParam String query, @RequestParam(defaultValue="") String prefixes)
            throws IOException, RepositoryException, RepositoryException, MalformedQueryException {
        if(!prefixes.trim().equals("")){
            query = prefixes.concat("\n").concat(query);
        }

        response.setContentType("text/plain");
        RepositoryConnection repCon = null;
        try {
            repCon = this.repository.getConnection();
            ParsedOperation pO = QueryParserUtil.parseOperation(QueryLanguage.SPARQL, query, null);
            if(pO instanceof ParsedUpdate){
                response.getWriter().append(repCon.prepareUpdate(QueryLanguage.SPARQL, query).toString());
            }
            if(pO instanceof ParsedQuery){
                Query q = repCon.prepareQuery(QueryLanguage.SPARQL, query);
                if (q instanceof SemagrowSailTupleQuery) {

                        SemagrowSailTupleQuery qq = (SemagrowSailTupleQuery)q;
                        Dataset activeDataset = qq.getActiveDataset();
                        if (activeDataset != null) {
                        	 Set<URI> namedGraphs = activeDataset.getNamedGraphs();
                             if (namedGraphs != null && !namedGraphs.isEmpty()) {
                                 for (URI u  : namedGraphs) {
                                     qq.addIncludedSource(u);
                                 }
                             }
                        }

                        TupleExpr decomposedExpr = ((SemagrowTupleQuery) q).getDecomposedQuery();
                        response.getWriter().append(decomposedExpr.toString());

                }
            }
            response.getWriter().flush();
        } finally {
            if(repCon!=null){
                repCon.close();
            }
        }
    }
    
    @RequestMapping(value="/reloadConfig", method=RequestMethod.GET)
    public void reloadConfig(HttpServletResponse response, HttpServletRequest request) 
            throws IOException, RepositoryConfigException, RepositoryException, MalformedQueryException {
        if(request.getUserPrincipal()!=null && request.isUserInRole(CONSTANTS.WEBAPP.ROLES.ROLE_SEMAGROW_ADMIN)){
            this.startUp();                    
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
        } else {
            if(request.getUserPrincipal()==null){
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            } else {
                response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            }
        }
    }    
    
    @ExceptionHandler({SemaGrowException.class})
    public void handleSemaGrowException(HttpServletResponse response, SemaGrowException e) throws IOException {           
        LOG.debug(e.getCause() != null ? e.getCause().getMessage() : e.getMessage(), e);
        response.sendError(e.getResponseCode(), e.getCause()!=null?e.getCause().getMessage():e.getMessage());
    }
    
    @ExceptionHandler({OpenRDFException.class})
    public void handleOpenRDFException(HttpServletResponse response, OpenRDFException e) throws IOException {
        LOG.debug(e.getCause()!=null?e.getCause().getMessage():e.getMessage(), e);        
        if(e instanceof MalformedQueryException){
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getCause()!=null?e.getCause().getMessage():e.getMessage());
        } else {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getCause()!=null?e.getCause().getMessage():e.getMessage());
        }
    }    
    
    private void handleQuery(OutputStream out, String accept, Query query)             
            throws IOException, RepositoryException, MalformedQueryException, QueryEvaluationException, 
                   TupleQueryResultHandlerException, RDFHandlerException, SemaGrowNotAcceptableException, SemaGrowTimeOutException, SemaGrowInternalException, SemaGrowExternalError, SemaGrowBadRequestException, Throwable {
        try {
            if(query instanceof TupleQuery){

                if(accept.indexOf(CONSTANTS.MIMETYPES.SPARQLRESULTS_XML)!=-1){
                    ((TupleQuery)query).evaluate(new SPARQLResultsXMLWriter(out));
                } else
                if(accept.indexOf(CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON)!=-1){
                    ((TupleQuery)query).evaluate(new SPARQLResultsJSONWriter(out));
                } else {
                    ((TupleQuery)query).evaluate(new HTMLTableWriter(out));
                }
            } else
            if(query instanceof GraphQuery){
                if(accept.indexOf(CONSTANTS.MIMETYPES.RDF_RDFXML)!=-1){
                    ((GraphQuery)query).evaluate(new RDFXMLPrettyWriter(out));
                } else
                if(accept.indexOf(CONSTANTS.MIMETYPES.RDF_N3)!=-1){
                    ((GraphQuery)query).evaluate(new N3Writer(out));
                } else
                if(accept.indexOf(CONSTANTS.MIMETYPES.RDF_TURTLE)!=-1){
                    ((GraphQuery)query).evaluate(new TurtleWriter(out));
                } else
                if(accept.indexOf(CONSTANTS.MIMETYPES.RDF_TRIG)!=-1){
                    ((GraphQuery)query).evaluate(new TriGWriter(out));
                } else
                if(accept.indexOf(CONSTANTS.MIMETYPES.RDF_TRIX)!=-1){
                    ((GraphQuery)query).evaluate(new TriXWriter(out));
                } else {
                    ((GraphQuery)query).evaluate(new TurtleWriter(out));
                }                
            } else
            if(query instanceof BooleanQuery){
                out.write((((BooleanQuery)query).evaluate()+"").getBytes());
            }
        } catch (TupleQueryResultHandlerException e) {
            throw new SemaGrowNotAcceptableException(e.getMessage()!=null?e.getMessage():"The requested type could not be handled by a source", e);
        } catch (QueryEvaluationException e) {            
            Throwable t = ExceptionUtils.getRootCause(e);            
            if(t instanceof QueryInterruptedException){                    
                throw new SemaGrowTimeOutException(e.getMessage()!=null?e.getMessage():"Query was interrupted",t);
            } else
            if(t instanceof QueryDecompositionException){
                throw new SemaGrowInternalException(e.getMessage()!=null?e.getMessage():"Internal Error",t);
            } else 
            if (t instanceof RdbmsQueryEvaluationException) {                
                throw new SemaGrowExternalError(e.getMessage()!=null?e.getMessage():"Unknown error during RDBMS query evaluation in source", t);
            } else 
            if (t instanceof UnsupportedRdbmsOperatorException) {                
                throw new SemaGrowBadRequestException(e.getMessage()!=null?e.getMessage():"Query cannot be handled by a RDBMS source", t);
            } else 
            if (t instanceof ValueExprEvaluationException) {                
                throw new SemaGrowNotAcceptableException(e.getMessage()!=null?e.getMessage():"The requested value expression could not be handled by a source", t);
            } else 
            if (t instanceof HTTPQueryEvaluationException){
                if(((HTTPQueryEvaluationException)e).isCausedByIOException()){
                    throw new SemaGrowTimeOutException(e.getMessage()!=null?e.getMessage():"Time-out because of I/O issues with the source", t);
                } else
                if(((HTTPQueryEvaluationException)e).isCausedByMalformedQueryException()){
                    throw new SemaGrowBadRequestException(e.getMessage()!=null?e.getMessage():"Query cannot be handled by a source (malformed query", t); 
                } else
                if(((HTTPQueryEvaluationException)e).isCausedByRepositoryException()){
                    throw new SemaGrowTimeOutException(e.getMessage()!=null?e.getMessage():"Time-out because of source repository issues", t);                    
                } else {
                    throw new SemaGrowExternalError(e.getMessage()!=null?e.getMessage():"Unknown error during Http query evaluation in source", t);
                }                             
            } else {
                throw t;
            }            
        } catch (RDFHandlerException e) {
            throw new SemaGrowExternalError(e.getMessage()!=null?e.getMessage():"Error during rdf handling", e);
        } catch (IOException e) {            
            throw new SemaGrowExternalError(e.getMessage()!=null?e.getMessage():"Error caused by I/O issues between SemaGrow and a source", e);
        }         
    }


    private class SyncTupleResultHandler implements TupleQueryResultHandler {

        private TupleQueryResultHandler handler;
        private CountDownLatch latch;

        public SyncTupleResultHandler(TupleQueryResultHandler inner, CountDownLatch countDownLatch) { handler = inner; latch = countDownLatch; }

        @Override
        public void handleBoolean(boolean b) throws QueryResultHandlerException {
            handler.handleBoolean(b);
        }

        @Override
        public void handleLinks(List<String> list) throws QueryResultHandlerException {
            handler.handleLinks(list);
        }

        @Override
        public void startQueryResult(List<String> list) throws TupleQueryResultHandlerException {
            handler.startQueryResult(list);
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
            handler.endQueryResult();
            latch.countDown();
        }

        @Override
        public void handleSolution(BindingSet bindingSet) throws TupleQueryResultHandlerException {
            handler.handleSolution(bindingSet);
        }

    }

}
