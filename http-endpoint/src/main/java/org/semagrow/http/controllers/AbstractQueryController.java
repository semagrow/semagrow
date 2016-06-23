package org.semagrow.http.controllers;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.common.lang.service.FileFormatServiceRegistry;
import org.eclipse.rdf4j.common.webapp.util.HttpServerUtil;
import org.eclipse.rdf4j.http.protocol.Protocol;
import org.eclipse.rdf4j.http.protocol.error.ErrorInfo;
import org.eclipse.rdf4j.http.protocol.error.ErrorType;
import org.eclipse.rdf4j.http.server.ClientHTTPException;
import org.eclipse.rdf4j.http.server.HTTPException;
import org.eclipse.rdf4j.http.server.ProtocolUtil;
import org.eclipse.rdf4j.http.server.ServerHTTPException;
import org.eclipse.rdf4j.http.server.repository.BooleanQueryResultView;
import org.eclipse.rdf4j.http.server.repository.GraphQueryResultView;
import org.eclipse.rdf4j.http.server.repository.QueryResultView;
import org.eclipse.rdf4j.http.server.repository.TupleQueryResultView;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.impl.SimpleDataset;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultWriterRegistry;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.config.RepositoryResolver;
import org.eclipse.rdf4j.rio.RDFWriterRegistry;
import org.semagrow.repository.SemagrowRepositoryResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.mvc.Controller;
import org.springframework.web.servlet.support.WebContentGenerator;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static javax.servlet.http.HttpServletResponse.*;
import static org.eclipse.rdf4j.http.protocol.Protocol.*;

/**
 * Created by angel on 17/6/2016.
 */
public abstract class AbstractQueryController extends WebContentGenerator implements Controller {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected RepositoryResolver resolver;
    private Repository repository;
    private RepositoryConnection connection;

    public AbstractQueryController() {
        resolver = new SemagrowRepositoryResolver();
    }

    public RepositoryResolver getRepositoryResolver() { return resolver; }

    public void setRepositoryResolver(RepositoryResolver resolver) { this.resolver = resolver; }

    protected abstract ModelAndView handleQuery(Query query, boolean headersOnly, HttpServletRequest request, HttpServletResponse response)  throws HTTPException;

    @Override
    public ModelAndView handleRequest(HttpServletRequest request, HttpServletResponse response)
            throws Exception
    {
        String reqMethod = request.getMethod();
        String queryStr = request.getParameter(QUERY_PARAM_NAME);

        if (METHOD_POST.equals(reqMethod)) {
            String mimeType = HttpServerUtil.getMIMEType(request.getContentType());

            if (!(FORM_MIME_TYPE.equals(mimeType) || SPARQL_QUERY_MIME_TYPE.equals(mimeType))) {
                throw new ClientHTTPException(SC_UNSUPPORTED_MEDIA_TYPE, "Unsupported MIME type: " + mimeType);
            }

            if (SPARQL_QUERY_MIME_TYPE.equals(mimeType)) {
                // The query should be the entire body
                try {
                    queryStr = IOUtils.toString(request.getReader());
                }
                catch (IOException e) {
                    throw new HTTPException(HttpStatus.SC_BAD_REQUEST, "Error reading request message body", e);
                }
                if (queryStr.isEmpty()) queryStr = null;
            }
        }

        int qryCode = 0;
        if (logger.isInfoEnabled() || logger.isDebugEnabled()) {
            qryCode = String.valueOf(queryStr).hashCode();
        }

        boolean headersOnly = false;
        if (METHOD_GET.equals(reqMethod)) {
            logger.info("GET query {}", qryCode);
        }
        else if (METHOD_HEAD.equals(reqMethod)) {
            logger.info("HEAD query {}", qryCode);
            headersOnly = true;
        }
        else if (METHOD_POST.equals(reqMethod)) {
            logger.info("POST query {}", qryCode);
        }

        logger.debug("query {} = {}", qryCode, queryStr);

        if (queryStr != null) {

            Repository repository = getRepository(request);
            RepositoryConnection repositoryCon = getRepositoryConnection(request);

            synchronized (repositoryCon) {
                Query query = getQuery(repository, repositoryCon, queryStr, request, response);

                return handleQuery(query, headersOnly, request, response);
            }
        }
        else {
            throw new ClientHTTPException(SC_BAD_REQUEST, "Missing parameter: " + QUERY_PARAM_NAME);
        }
    }

    private RepositoryConnection getRepositoryConnection(HttpServletRequest request) {

        if (connection == null) {
            Repository repository = getRepository(request);
            connection = repository.getConnection();
        }

        return connection;
    }

    private Repository getRepository(HttpServletRequest request)
    {
        if (repository == null) {
            repository = resolver.getRepository(null);
        }

        return repository;
    }

    private Query getQuery(Repository repository, RepositoryConnection repositoryCon, String queryStr,
                           HttpServletRequest request, HttpServletResponse response)
            throws IOException, ClientHTTPException
    {
        Query result = null;

        // default query language is SPARQL
        QueryLanguage queryLn = QueryLanguage.SPARQL;

        String queryLnStr = request.getParameter(QUERY_LANGUAGE_PARAM_NAME);
        logger.debug("query language param = {}", queryLnStr);

        if (queryLnStr != null) {
            queryLn = QueryLanguage.valueOf(queryLnStr);

            if (queryLn == null) {
                throw new ClientHTTPException(SC_BAD_REQUEST, "Unknown query language: " + queryLnStr);
            }
        }

        String baseURI = request.getParameter(BASEURI_PARAM_NAME);

        // determine if inferred triples should be included in query evaluation
        boolean includeInferred = ProtocolUtil.parseBooleanParam(request, INCLUDE_INFERRED_PARAM_NAME, true);

        String timeout = request.getParameter(TIMEOUT_PARAM_NAME);
        int maxQueryTime = 0;
        if (timeout != null) {
            try {
                maxQueryTime = Integer.parseInt(timeout);
            }
            catch (NumberFormatException e) {
                throw new ClientHTTPException(SC_BAD_REQUEST, "Invalid timeout value: " + timeout);
            }
        }

        // build a dataset, if specified
        String[] defaultGraphURIs = request.getParameterValues(DEFAULT_GRAPH_PARAM_NAME);
        String[] namedGraphURIs = request.getParameterValues(NAMED_GRAPH_PARAM_NAME);

        SimpleDataset dataset = null;
        if (defaultGraphURIs != null || namedGraphURIs != null) {
            dataset = new SimpleDataset();

            if (defaultGraphURIs != null) {
                for (String defaultGraphURI : defaultGraphURIs) {
                    try {
                        IRI uri = createURIOrNull(repository, defaultGraphURI);
                        dataset.addDefaultGraph(uri);
                    }
                    catch (IllegalArgumentException e) {
                        throw new ClientHTTPException(SC_BAD_REQUEST, "Illegal URI for default graph: "
                                + defaultGraphURI);
                    }
                }
            }

            if (namedGraphURIs != null) {
                for (String namedGraphURI : namedGraphURIs) {
                    try {
                        IRI uri = createURIOrNull(repository, namedGraphURI);
                        dataset.addNamedGraph(uri);
                    }
                    catch (IllegalArgumentException e) {
                        throw new ClientHTTPException(SC_BAD_REQUEST, "Illegal URI for named graph: "
                                + namedGraphURI);
                    }
                }
            }
        }

        try {
            result = repositoryCon.prepareQuery(queryLn, queryStr, baseURI);

            result.setIncludeInferred(includeInferred);

            if (maxQueryTime > 0) {
                result.setMaxQueryTime(maxQueryTime);
            }

            if (dataset != null) {
                result.setDataset(dataset);
            }

            // determine if any variable bindings have been set on this query.
            @SuppressWarnings("unchecked")
            Enumeration<String> parameterNames = request.getParameterNames();

            while (parameterNames.hasMoreElements()) {
                String parameterName = parameterNames.nextElement();

                if (parameterName.startsWith(BINDING_PREFIX) && parameterName.length() > BINDING_PREFIX.length())
                {
                    String bindingName = parameterName.substring(BINDING_PREFIX.length());
                    Value bindingValue = ProtocolUtil.parseValueParam(request, parameterName,
                            repository.getValueFactory());
                    result.setBinding(bindingName, bindingValue);
                }
            }
        }
        catch (UnsupportedQueryLanguageException e) {
            ErrorInfo errInfo = new ErrorInfo(ErrorType.UNSUPPORTED_QUERY_LANGUAGE, queryLn.getName());
            throw new ClientHTTPException(SC_BAD_REQUEST, errInfo.toString());
        }
        catch (MalformedQueryException e) {
            ErrorInfo errInfo = new ErrorInfo(ErrorType.MALFORMED_QUERY, e.getMessage());
            throw new ClientHTTPException(SC_BAD_REQUEST, errInfo.toString());
        }
        catch (RepositoryException e) {
            logger.error("Repository error", e);
            response.sendError(SC_INTERNAL_SERVER_ERROR);
        }

        return result;
    }

    private IRI createURIOrNull(Repository repository, String graphURI) {
        if ("null".equals(graphURI))
            return null;
        return repository.getValueFactory().createIRI(graphURI);
    }
}
