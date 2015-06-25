/**
 * Module vocabulary provides only the most necessary RDF primitives,
 * so that they don't need be hardcoded
 * @module vocabulary
 */
/**
 * RDF : only the most common RDF primitives
 * @class RDF
 * @static
 */
RDF = {
    /**
     * rdf:type
     * @property TYPE
     * @type URI
     */
    TYPE:new URI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
    /**
     * rdf:Subject
     * @property SUBJECT
     * @type URI
     */
    SUBJECT:new URI("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject"),
    /**
     * rdf:Predicate
     * @property PREDICATE
     * @type URI
     */
    PREDICATE:new URI("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate"),
    /**
     * rdf:Object
     * @property Object
     * @type URI
     */
    OBJECT:new URI("http://www.w3.org/1999/02/22-rdf-syntax-ns#object")    
}

/**
 * DOAP : only the most common DOAP primitives
 * @class DOAP
 * @static
 */
SIOC = {
    /**
     * sioc:Space
     * @property SPACE
     * @type URI
     */    
    SPACE:new URI("http://rdfs.org/sioc/ns#Space")
}

/**
 * RDFS : only the most common RDFS primitives
 * @class RDFS
 * @static
 */
RDFS = {
    /**
     * rdfs:label
     * @property LABEL
     * @type URI
     */
    LABEL:new URI("http://www.w3.org/2000/01/rdf-schema#label"),
    /**
     * rdfs:subPropertyOf
     * @property SUBPROPERTYOF
     * @type URI
     */
    SUBPROPERTYOF:new URI("http://www.w3.org/2000/01/rdf-schema#subPropertyOf"),
    /**
     * rdfs:comment
     * @property COMMENT
     * @type URI
     */
    COMMENT:new URI("http://www.w3.org/2000/01/rdf-schema#comment")
}

/**
 * OWL : only the most common OWL primitives
 * @class OWL
 * @static
 */
OWL = {
    /**
     * owl:inverseOf
     * @property INVERSEOF
     * @type URI
     */
    INVERSEOF:new URI("http://www.w3.org/2002/07/owl#inverseOf")
}

/**
 * SKOS : only the most common SKOS primitives
 * @class SKOS 
 * @static
 */
SKOS = {
    /**
     * skos:Collection
     * @property COLLECTION
     * @type URI
     */
    COLLECTION:new URI("http://www.w3.org/2004/02/skos/core#Collection"),    
    /**
     * skos:member
     * @property MEMBER
     * @type URI
     */
    MEMBER:new URI("http://www.w3.org/2004/02/skos/core#member"),        
    /**
     * skos:Concept
     * @property CONCEPT
     * @type URI
     */
    CONCEPT:new URI("http://www.w3.org/2004/02/skos/core#Concept"),
    /**
     * skos:ConceptScheme
     * @property CONCEPTSCHEME
     * @type URI
     */
    CONCEPTSCHEME:new URI("http://www.w3.org/2004/02/skos/core#ConceptScheme"),
    /**
     * skos:hasTopConcept
     * @property HASTOPCONCEPT
     * @type URI
     */
    HASTOPCONCEPT:new URI("http://www.w3.org/2004/02/skos/core#hasTopConcept"),
    /**
     * skos:topConceptOf
     * @property TOPCONCEPTOF
     * @type URI
     */
    TOPCONCEPTOF:new URI("http://www.w3.org/2004/02/skos/core#topConceptOf"),    
    /**
     * skos:prefLabel
     * @property PREFLABEL
     * @type URI
     */
    PREFLABEL:new URI("http://www.w3.org/2004/02/skos/core#prefLabel"),
    /**
     * skos:altLabel
     * @property ALTLABEL
     * @type URI
     */
    ALTLABEL:new URI("http://www.w3.org/2004/02/skos/core#altLabel"),
    /**
     * skos:hiddenLabel
     * @property HIDDENLABEL
     * @type URI
     */
    HIDDENLABEL:new URI("http://www.w3.org/2004/02/skos/core#hiddenLabel"),
    /**
     * skos:definition
     * @property DEFINITION
     * @type URI
     */
    DEFINITION:new URI("http://www.w3.org/2004/02/skos/core#definition"),
    /**
     * skos:scopeNote
     * @property SCOPENOTE
     * @type URI
     */
    SCOPENOTE:new URI("http://www.w3.org/2004/02/skos/core#scopeNote"),
    /**
     * skos:narrower
     * @property NARROWER
     * @type URI
     */
    NARROWER:new URI("http://www.w3.org/2004/02/skos/core#narrower"),
    /**
     * skos:broader
     * @property BROADER
     * @type URI
     */
    BROADER:new URI("http://www.w3.org/2004/02/skos/core#broader"),
    /**
     * skos:related
     * @property RELATED
     * @type URI
     */
    RELATED:new URI("http://www.w3.org/2004/02/skos/core#related"),
    /**
     * skos:exactMatch
     * @property EXACTMATCH
     * @type URI
     */
    EXACTMATCH:new URI("http://www.w3.org/2004/02/skos/core#exactMatch"),
    /**
     * skos:closeMatch
     * @property CLOSEMATCH
     * @type URI
     */
    CLOSEMATCH:new URI("http://www.w3.org/2004/02/skos/core#closeMatch"),
    /**
     * skos:relatedMatch
     * @property RELATEDMATCH
     * @type URI
     */
    RELATEDMATCH:new URI("http://www.w3.org/2004/02/skos/core#relatedMatch"),
    /**
     * skos:broadMatch
     * @property BROADMATCH
     * @type URI
     */
    BROADMATCH:new URI("http://www.w3.org/2004/02/skos/core#broadMatch"),
    /**
     * skos:narrowMatch
     * @property NARROWMATCH
     * @type URI
     */
    NARROWMATCH:new URI("http://www.w3.org/2004/02/skos/core#narrowMatch"),
    /**
     * skos:closeMatch
     * @property CLOSEMATCH
     * @type URI
     */
    NOTATION:new URI("http://www.w3.org/2004/02/skos/core#notation"),
    /**
     * skos:example
     * @property EXAMPLE
     * @type URI
     */
    EXAMPLE:new URI("http://www.w3.org/2004/02/skos/core#example")
}

/**
 * DC: only the most common DC primitives
 * @class DC
 * @static
 */
DC = {
    /**
     * dc:title
     * @property TITLE
     * @type URI
     */
    TITLE:new URI("http://purl.org/dc/elements/1.1/title"),
    /**
     * dc:description
     * @property DESCRIPTION
     * @type URI
     */
    DESCRIPTION:new URI("http://purl.org/dc/elements/1.1/description")
}

/**
 * DCTERMS: only the most common DCTERMS primitives
 * @class DCTERMS
 * @static
 */
DCTERMS = {
    /**
     * dcterms:title
     * @property TITLE
     * @type URI
     */
    TITLE:new URI("http://purl.org/dc/terms/title"),
    /**
     * dcterms:description
     * @property DESCRIPTION
     * @type URI
     */
    DESCRIPTION:new URI("http://purl.org/dc/terms/description"),
    /**
     * dcterms:license
     * @property LICENSE
     * @type URI
     */
    LICENSE:new URI("http://purl.org/dc/terms/license"),
    /**
     * dcterms:subject
     * @property SUBJECT
     * @type URI
     */
    SUBJECT:new URI("http://purl.org/dc/terms/subject"),
    /**
     * dcterms:creator
     * @property CREATOR
     * @type URI
     */
    CREATOR:new URI("http://purl.org/dc/terms/creator"),
    /**
     * dcterms:publisher
     * @property PUBLISHER
     * @type URI
     */
    PUBLISHER:new URI("http://purl.org/dc/terms/publisher"),
    /**
     * dcterms:contributor
     * @property CONTRIBUTOR
     * @type URI
     */
    CONTRIBUTOR:new URI("http://purl.org/dc/terms/contributor"),
    /**
     * dcterms:created
     * @property CREATED
     * @type URI
     */
    CREATED:new URI("http://purl.org/dc/terms/created")

}

/**
 * FOAF : only the most common FOAF primitives
 * @class FOAF
 * @static
 */
FOAF = {
    /**
     * foaf:homepage
     * @property HOMEPAGE
     * @type URI
     */
    HOMEPAGE:new URI("http://xmlns.com/foaf/0.1/homepage")
}

/**
 * VOID : only the most common VOID primitives
 * @class VOID
 * @static
 */
VOID = {
    /**
     * void:exampleResource
     * @property EXAMPLERESOURCE
     * @type URI
     */
    EXAMPLERESOURCE:new URI("http://rdfs.org/ns/void#exampleResource")
}

/**
 * XMLSCHEMA: only the most common XMLSCHEMA primitives
 * @class XMLSCHEMA
 * @static
 */
XMLSCHEMA = {
    /**
     * xsd:dateTime
     * @property DATETIME
     * @type URI
     */                      
    DATETIME:new URI("http://www.w3.org/2001/XMLSchema#dateTime"),
    /**
     * xsd:string
     * @property STRING
     * @type URI
     */
    STRING:new URI("http://www.w3.org/2001/XMLSchema#string"),
    /**
     * xsd:integer
     * @property INTEGER
     * @type URI
     */
    INTEGER:new URI("http://www.w3.org/2001/XMLSchema#integer"),    

    DOUBLE:new URI("http://www.w3.org/2001/XMLSchema#double"),
    INT:new URI("http://www.w3.org/2001/XMLSchema#int"),
    BOOLEAN:new URI("http://www.w3.org/2001/XMLSchema#boolean")        

}
/**
 * CHANGESET : only the most common CHANGESET primitives
 * @class CHANGESET
 * @static
 */
CHANGESETS = {
    /**
     * changeset:Project
     * @property PROJECTS
     * @type URI
     */									
    CHANGESET:new URI("http://purl.org/vocab/changeset/schema#ChangeSet"),
    /**
     * changeset:addition
     * @property ADDITION
     * @type URI
     */									
    ADDITION:new URI("http://purl.org/vocab/changeset/schema#addition"), 
    /**
     * changeset:removal
     * @property REMOVAL
     * @type URI
     */									
    REMOVAL:new URI("http://purl.org/vocab/changeset/schema#removal"),
    /**
     * changeset:graph
     * @property GRAPH
     * @type URI
     */									
    GRAPH:new URI("http://purl.org/vocab/changeset/schema#graph")     
}

