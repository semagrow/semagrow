<%@page import="eu.semagrow.commons.CONSTANTS"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>


<div id="sparqlContentLeft">
    <form id="sparqlQuery" method="POST">
        <textarea name="prefixes" id="prefixes">
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl:<http://www.w3.org/2002/07/owl#>
PREFIX skos:<http://www.w3.org/2004/02/skos/core#>        
</textarea>        
<textarea name="<%=CONSTANTS.WEBAPP.PARAM_QUERY%>" id="<%=CONSTANTS.WEBAPP.PARAM_QUERY%>">SELECT * WHERE {
  ?s ?p ?o
} LIMIT 20</textarea><br/>
        <button type="button" onclick="javascript:SemaGrowSparql.runSparqlQuery()">SPARQL</button>
        <select name="<%=CONSTANTS.WEBAPP.PARAM_ACCEPT%>" id="<%=CONSTANTS.WEBAPP.PARAM_ACCEPT%>">
            <optgroup label="SELECT">
                <option value="<%=CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON%>"><%=CONSTANTS.MIMETYPES.SPARQLRESULTS_JSON%></option>
                <option value="<%=CONSTANTS.MIMETYPES.SPARQLRESULTS_XML%>"><%=CONSTANTS.MIMETYPES.SPARQLRESULTS_XML%></option>
                <option value="<%=CONSTANTS.MIMETYPES.TEXT_HTML%>"><%=CONSTANTS.MIMETYPES.TEXT_HTML%></option>
            </optgroup>
            <optgroup label="CONSTRUCT">
                <option value="<%=CONSTANTS.MIMETYPES.RDF_TURTLE%>"><%=CONSTANTS.MIMETYPES.RDF_TURTLE%></option>
                <option value="<%=CONSTANTS.MIMETYPES.RDF_RDFXML%>"><%=CONSTANTS.MIMETYPES.RDF_RDFXML%></option>
                <option value="<%=CONSTANTS.MIMETYPES.RDF_N3%>"><%=CONSTANTS.MIMETYPES.RDF_N3%></option>
                <option value="<%=CONSTANTS.MIMETYPES.RDF_NQUADS%>"><%=CONSTANTS.MIMETYPES.RDF_NQUADS%></option>
                <option value="<%=CONSTANTS.MIMETYPES.RDF_NTRIPLES%>"><%=CONSTANTS.MIMETYPES.RDF_NTRIPLES%></option>
                <option value="<%=CONSTANTS.MIMETYPES.RDF_TRIG%>"><%=CONSTANTS.MIMETYPES.RDF_TRIG%></option>
                <option value="<%=CONSTANTS.MIMETYPES.RDF_TRIX%>"><%=CONSTANTS.MIMETYPES.RDF_TRIX%></option>                
            </optgroup>
            <optgroup label="ASK">
                <option value="<%=CONSTANTS.MIMETYPES.TEXT_PLAIN%>"><%=CONSTANTS.MIMETYPES.TEXT_PLAIN%></option>
            </optgroup>
        </select>
        <br/>
        <button type="button" onclick="javascript:SemaGrowSparql.explainSparqlQuery()">Explain</button>
        <button type="button">Decompose Query</button>
        <button type="button" onclick="javascript:SemaGrowSparql.explainDecomposedQuery()">Explain decomposed Query</button>
    </form>
    <div id="sparqlSamplesContainer">SAMPLES</div>
</div>
<div id="sparqlContentRight">
    <div id="sparqlResponse"></div>
</div>
            
                      
