/**
 * The model module provides basic dataTypes of the OpenRDF API
 * @module model
 */

/**
 * The javascript representation of an openrdf.model.Value
 * @class Value
 * @constructor
 */
Value = function(){
};

Value.prototype = {
    /**
     * Clones a Value
     * @method cloneValue
     * @return { Value || Resource || Literal || URI || Bnode } the clone of this Value
     */
    cloneValue:function(){
        var clone = new this.constructor();
        for (var i in this) {
           clone[i] = this[i];
        }
        return clone;
    }
};
/**
 * The javascript representation of an openrdf.model.Resource
 * @class Resource
 * @constructor
 * @extends Value
 */
Resource = function(){
    Resource.superclass.constructor.apply(this);
};

YAHOO.extend(Resource, Value);

/**
 * The javascript representation of an openrdf.model.Statement. Please note that web statements always have
 * an id as key in the collection of statements. Either the elementID they come from or a new UUID.
 * @class Statement
 * @constructor
 * @param  Resource_subject {Resource} the Subject
 * @param  URI_predicate {URI} the Predicate
 * @param  Value_object {Value} the Object
 */
Statement = function(Resource_subject, URI_predicate, Value_object){
    /**
     * The subject
     * @property subj
     * @type Resource          
     */    
    this.subj = Resource_subject;
    /**
     * The predicate
     * @property pred
     * @type URI
     */
    this.pred = URI_predicate;
    /**
     * The object
     * @property obj
     * @type Value          
     */
    this.obj = Value_object;
};
Statement.prototype = {
        /**
         * Returns the subject of this statement
         * @method getSubject
         * @return { Resource } the subject as Resource         
         */
	getSubject:function(){
		return this.subj;
	},
        /**
         * Returns the predicate of this statement
         * @method getPredicate
         * @return { URI } the predicate as Resource
         */
	getPredicate:function(){
		return this.pred;
	},
        /**
         * Returns the object of this statement
         * @method getObject
         * @return { Value } the object as Resource
         */
	getObject:function(){
		return this.obj;
	},
        /**
         * Returns a String representation of this statement
         * @method toString
         * @return { string } the Statement as string
         */
	toString:function(){
		return this.subj + " " + this.pred + "  " + this.obj;
	}
};

/**
 * The javascript representation of an openrdf.model.Namespace
 * @class Namespace
 * @constructor
 * @param  String_prefix {string} the prefix of the namespace
 * @param  String_namespace {string} the namespace
 */
Namespace = function(String_prefix, String_namespace){
    /**
     * The prefix for this namespace
     * @property prefix
     * @type string
     */
    this.prefix = String_prefix;
    /**
     * The namespace itself
     * @property namespace
     * @type string
     */
    this.namespace = String_namespace;
}
Namespace.prototype = {
    /**
     * Returns the prefix of this namespace
     * @method getPrefix
     * @return { string } the prefix
     */
    getPrefix:function(){
        return this.prefix;
    },
    /**
     * Returns the namespace
     * @method getNamespace
     * @return { string } the namespace
     */
    getNamespace:function(){
            return this.namespace;
    }
};

(function () {
    /**
     * The javascript representation of an openrdf.model.Literal
     * @class Literal
     * @constructor
     * @extends Value
     * @param sLabel {string} the label of this literal
     * @param oLanguageOrDataType { string || URI } the language or the datatype as URI
     */
    Literal = function(sLabel, oLanguageOrDataType){        
        Literal.superclass.constructor.apply(this);
        /**
         * The label of this Literal
         * @property label
         * @type string
         */
        this.label = sLabel;
        if(oLanguageOrDataType){
            if(oLanguageOrDataType instanceof URI){
                /**
                 * The datatype of this Literal, which only gets created, if the second argument of the constructor
                 * is an URI, will be undefined otherwise
                 * @property datatype
                 * @type URI
                 */
                this.datatype = oLanguageOrDataType;
            } else {
                /**
                 * The language of this Literal, which only gets created, if the second argument of the constructor
                 * is a string, will be undefined otherwise
                 * @property language
                 * @type string
                 */
                this.language = oLanguageOrDataType;
            }

        }                
    };
    YAHOO.extend(Literal, Value, {
        /**
         * Returns the label of this Literal
         * @method getLabel
         * @return { string } the label
         */
        getLabel:function(){
            return this.label;
        },
        /**
         * Returns the language of this Literal
         * @method getLanguage
         * @return { string || undefined } the language
         */
        getLanguage:function(){
            return this.language;
        },
        /**
         * Returns the datatype of this Literal
         * @method getDataType
         * @return { URI || undefined } the datatype
         */
        getDataType:function(){
            return this.datatype;
        },
        /**
         * Returns a string representation of this Literal
         * @method toString
         * @return { string } the Literal as string
         */
        toString:function(){
            if(this.language){
                return "\""+this.label+"\""+this.language;
            } else
            if(this.dataType){
                return "\""+this.label+"\"^^"+this.dataType;
            } else {
                return "\""+this.label+"\"";
            }
        },
        /**
         * Returns the label of this Literal
         * @method stringValue
         * @return { string } the label
         */
        stringValue:function(){
            return this.label;
        }
    });
})();


(function () {
    /**
     * The javascript representation of an openrdf.model.URI
     * @class URI
     * @constructor
     * @extends Value
     * @param sURI {string} the stringValue of this URI
     */
    URI = function(sURI){
        URI.superclass.constructor.apply(this);
        /**
         * The uri as string of this URI
         * @property uri
         * @type string
         */
        this.uri = "";
        this._setURI(sURI);
    };
    YAHOO.extend(URI, Resource, {
        /**
         * Sets the uri string of this URI and calculates localname and namespace
         * @method setURI
         * @param sURI { string }
         * @private
         */
        _setURI:function(sURI){
            this.uri = sURI;
            try {
                this.getLocalName();
                this.getNamespace();
            } catch(e){}
        },
        /**
         * Returns the localName of this URI
         * @method getLocalName
         * @return { string } the localName
         */
        getLocalName:function(){
            this.localname = this.uri.substring(this._getLocalNameIndex());
            return this.localname;
        },
        /**
         * Returns the namespace of this URI
         * @method getNamespace
         * @return { string } the namespace
         */
        getNamespace:function(){
            this.namespace = this.uri.substring(0, this._getLocalNameIndex());
            return this.namespace;
        },
        /**
         * Returns a string representation of this URI
         * @method toString
         * @return { string } the URI as string
         */
        toString:function(){
            return this.uri;
        },
        /**
         * Returns a string representation of this URI
         * @method stringValue
         * @return { string } the URI as string
         */
        stringValue:function(){
            return this.uri;
        },
        /**
         * Returns the index where to split this URI to get localName and namespace
         * @method _getLocalNameIndex
         * @return { int }  the index
         * @private
         */
        _getLocalNameIndex:function(){            
            separatorIdx = this.uri.indexOf('#');
            if (separatorIdx < 0) {
                separatorIdx = this.uri.lastIndexOf('/');
            }
            if (separatorIdx < 0) {
                separatorIdx = this.uri.lastIndexOf(':');
            }
            if (separatorIdx < 0) {                
                throw "No separator character founds in URI: " + this.uri;
            }
            return separatorIdx + 1;
        }
    });
})();


(function () {
/**
 * The javascript representation of an openrdf.model.BNode
 * @class BNode
 * @constructor
 * @extends Value
 * @param sID {string} the id of this BNode
 */
BNode = function(sID){
    BNode.superclass.constructor.apply(this);
    /**
     * The id as string of this BNode
     * @property id
     * @type string
     */
    this.id = "";
    this._setID(sID);
};
YAHOO.extend(BNode, Value, {
    /**
     * Sets the id string of this BNode
     * @method _setID
     * @param sID { string }
     * @private
     */
    _setID:function(sID){
        this.id = sID;
    },
    /**
     * Returns the id of this BNode
     * @method getID
     * @return { string } the id
     */
    getID:function(){
        return this.id;
    },
    /**
     * Returns a string representation of this BNode
     * @method toString
     * @return { string } the BNode as string
     */
    toString:function(){
        return "_:"+this.id;
    },
    /**
     * Returns a string representation of this BNode
     * @method stringValue
     * @return { string } the BNode as string
     */
    stringValue:function(){
        return this.id;
    }
});
})();

/**
 * ValueConstants for empty Values
 * @class OpenRDFValueConstants
 * @constructor 
 */
OpenRDFValueConstants = {
    /**
     * An empty Literal. Call it like OpenRDFValueConstants.XSDSTRING
     * @property XSDSTRING
     * @type Literal
     */
    XSDSTRING:new Literal(""),
    /**
     * A empty xsd:dateTime Literal (http://www.w3.org/2001/XMLSchema#dateTime).
     * Call it like OpenRDFValueConstants.XSDDATETIME
     * @property XSDDATETIME
     * @type Literal          
     */    
    XSDDATETIME:new Literal("", new URI("http://www.w3.org/2001/XMLSchema#dateTime")),
    /**
     * A empty xsd:boolean Literal (http://www.w3.org/2001/XMLSchema#boolean).
     * Call it like OpenRDFValueConstants.XSDBOOLEAN
     * @property XSDBOOLEAN
     * @type Literal
     */
    XSDBOOLEAN:new Literal("",new URI("http://www.w3.org/2001/XMLSchema#boolean")),
    /**
     * A empty xsd:float Literal (http://www.w3.org/2001/XMLSchema#float).
     * Call it like OpenRDFValueConstants.XSDFLOAT
     * @property XSDFLOAT
     * @type Literal          
     */     
    XSDFLOAT:new Literal("",new URI("http://www.w3.org/2001/XMLSchema#float")),
    /**
     * A empty xsd:int Literal (http://www.w3.org/2001/XMLSchema#int).
     * Call it like OpenRDFValueConstants.XSDINTEGER
     * @property XSDINTEGER
     * @type Literal
     */
    XSDINTEGER:new Literal("",new URI("http://www.w3.org/2001/XMLSchema#int")),
    /**
     * An empty URI
     * Call it like OpenRDFValueConstants.OPENRDFURI
     * @property OPENRDFURI
     * @type URI          
     */        
    OPENRDFURI:new URI(""),
    /**
     * An empty BNode
     * Call it like OpenRDFValueConstants.OPENRDFBNODE
     * @property OPENRDFBNODE
     * @type BNode
     */
    OPENRDFBNODE:new BNode("")
};
