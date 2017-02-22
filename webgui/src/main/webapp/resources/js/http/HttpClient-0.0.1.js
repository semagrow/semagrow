/**
 * The http module provides advanced augmentable http capabilities
 * @module http
 * @main http
 */
	/**
	 * @class MIMEFileExtensions
   * @static
	 */
	MIMEFileExtensions = {
  	  /**
	     * Get a default file extension for a given mime type
	     * @method getExtension
        * @param {String} sHTTPAccept The mime type to retrieve the default extension for
       * @return {String} the default file extension
       * @example var fileExtension = MIMEFileExtensions.getExtension('text/html');
       */			
		  getExtension: function(sHTTPAccept){
		      switch(sHTTPAccept){
		          case ("text/html") :
		              return "html";
		              break;
		          case "text/plain":
		              return "nt";
		              break;
		          case "text/velocity+html":
		              return "vhtml";
		              break;
		          case "text/pp+velocity+html":
		              return "ppvhtml";
		              break;
		          case "application/swc+javascript":
		              return "swcj";
		              break;
		          case "application/jsp":
		              return "jsp";
		              break;
		      }
		  }
	};
	/**
	 * HTTPAccept Constants
	 * @class HTTPAccept
	 * @static
	 */
	HTTPAccept = {
		  /**
		   * text/html
		   * @property HTML
		   * @type String
		   */
		  HTML: "text/html",
		  /**
		   * application/json
		   * @property JSON
		   * @type String
		   */
		  JSON: "application/json",
		  /**
		   * application/rdf+xml
		   * @property RDFXML
		   * @type String
		   */
		  RDFXML: "application/rdf+xml",
		  /**
		   * text/plain
		   * @property NTRIPLES
		   * @type String
		   */
		  NTRIPLES: "text/plain",
                  SPARQL_RESULTS_JSON: "application/sparql-results+json"
	};
	/**
	 * HTTPMethod Constants
	 * @class HTTPMethod
	 * @static
	 */
	HTTPMethod = {
		  /**
		   * POST
		   * @property POST
		   * @type String
		   */
		  POST: "POST",
		  /**
		   * GET
		   * @property GET
		   * @type String
		   */
		  GET: "GET",
		  /**
		   * PUT
		   * @property PUT
		   * @type String
		   */
		  PUT: "PUT",
		  /**
		   * DELETE
		   * @property DELETE
		   * @type String
		   */
		  DELETE: "DELETE",
		  /**
		   * HEAD
		   * @property HEAD
		   * @type String
		   */
		  HEAD: "HEAD"
	};
	/**
	 * HTTPCONST Constants for the HttpClient
	 * @class HTTPCONST
	 * @static
	 */
	HTTPCONST = {
		  /**
		   * COMPLETEMESSAGE : the key under which the message for the wait dialog is available from oRequestData's localData
		   * @property COMPLETEMESSAGE
		   * @type String
		   */
		  COMPLETEMESSAGE: "completemessage"
	};
	/**
	 * The HttpClient which can be augemented by other classes, so are http enabled
	 * @class HttpClient
	 * @static
	 */
	var HttpClient = {
		  /**
		   * The container for oRequestData identified by the transactionId
		   * @property RCS
		   * @type Object
		   */
		  RCS:new Object(),
		  /**
		   * Adds a RequestObject to RCS by key oConnectionObject.tId
		   * @method addRequestObject
		   * @param {String} tId The ID of this oConnectionObject.tId
		   * @param {Object} oRequestData
		   */
		  addRequestObject:function(tId, oRequestData){
		      this.RCS[tId] = oRequestData;
		  },
		  /**
		   * dials the webapp.
		   * @method _dial
		   * @param {String} sUrl the URL of the service
		   * @param {Object} oRequestData
		   * @param {String} sMethod one of HTTPMethod
		   * @param {String} sAccept one of HTTPAccept
		   * @private
		   */
		  _dial:function(sUrl, oRequestData, sMethod, sAccept) {            
		      YAHOO.util.Connect.resetDefaultHeaders();        
                      
		      YAHOO.util.Connect.initHeader("Accept",sAccept);        
		      if(oRequestData["headers"]!=null){
		          for(header in oRequestData["headers"]){
		              if(oRequestData["headers"]!="function"){
		                  YAHOO.util.Connect.initHeader(header,oRequestData["headers"][header]);
		              }
		          }
		      }
		      var conObj;                      
		      if(oRequestData["remoteData"]==null){
                          if(oRequestData["localData"]["form"]!==undefined){
                            var formObject = document.getElementById(oRequestData["localData"]["form"]); 
                                         YAHOO.util.Connect.setForm(formObject);
                                conObj = YAHOO.util.Connect.asyncRequest(formObject.getAttribute("method").toUpperCase(), sUrl, this);
                          } else {
                            conObj = YAHOO.util.Connect.asyncRequest(sMethod, sUrl, this, null);               
                          }		          
		      } else {
		          if(oRequestData["remoteData"]["content-type"]!==undefined){                
		              if(oRequestData["remoteData"]["content-type"]!=="application/json"){                    
		                  YAHOO.util.Connect.setDefaultPostHeader(false);
		                  YAHOO.util.Connect.initHeader("Content-Type", oRequestData["remoteData"]["content-type"], true);                                       
		                  conObj = YAHOO.util.Connect.asyncRequest(sMethod, sUrl, this, oRequestData["remoteData"]["requestContent"]);
		              } else {
		                  YAHOO.util.Connect.setDefaultPostHeader(false);
		                  YAHOO.util.Connect.initHeader("Content-Type", "application/json", true);                    
		                  conObj = YAHOO.util.Connect.asyncRequest(sMethod, sUrl, this, YAHOO.lang.JSON.stringify(oRequestData["remoteData"]["requestContent"]));
		              }
		          } else {
		              YAHOO.util.Connect.setDefaultPostHeader(false);
		              YAHOO.util.Connect.initHeader("Content-Type", "application/json", true);                                
		              conObj = YAHOO.util.Connect.asyncRequest(sMethod, sUrl, this, YAHOO.lang.JSON.stringify(oRequestData["remoteData"]));
		          }
		      }
		      this.addRequestObject(conObj.tId, oRequestData);
		      this.addWaitingFor(conObj.tId);
		  },
                  SUBMIT:function(sUrl, oRequestData, sAccept){
		      if(!sAccept){
		          sAccept = HTTPAccept.HTML;
		      }
		      this._dial(sUrl, oRequestData, HTTPMethod.POST, sAccept);                      
                  },
		  /**
		   * POSTs the given JSON to the given url using the given accept-header
		   * @method POST
		   * @param {String} sUrl the URL to POST to
		   * @param {Object} oRequestData the JSON to POST
		   * @param {String} sAccept the HTTPAccept method to use
		   */
		  POST:function(sUrl, oRequestData, sAccept){
		      if(!sAccept){
		          sAccept = HTTPAccept.JSON;
		      }
		      this._dial(sUrl, oRequestData, HTTPMethod.POST, sAccept);
		  },
		  /**
		   * GETs the given URL using the given JSON using the given accept-header
		   * @method GET
		   * @param {String} sUrl the URL to GET from
		   * @param {Object} oRequestData the JSON used as paramter
		   * @param {String} sAccept the HTTPAccept method to use
		   */
		  GET:function(sUrl, oRequestData, sAccept){
		      if(!sAccept){
		          sAccept = HTTPAccept.JSON;
		      }
		      this._dial(sUrl, oRequestData, HTTPMethod.GET, sAccept);
		  },
		  /**
		   * PUTs the given JSON to the given url using the given accept-header
		   * @method PUT
		   * @param {String} sUrl the URL to PUT to
		   * @param {Object} oRequestData the JSON to PUT
		   * @param {String} sAccept the HTTPAccept method to use
		   */
		  PUT:function(sUrl, oRequestData, sAccept){
		      if(!sAccept){
		          sAccept = HTTPAccept.JSON;
		      }
		      this._dial(sUrl, oRequestData, HTTPMethod.PUT, sAccept)
		  },
		  /**
		   * Call the DELETE method of the given URL using the given JSON as parameter using the given accept-header
		   * @method DELETE
		   * @param {String} sUrl the URL to send the DELETE request to
		   * @param {Object} oRequestData the JSON to use as parameter for this DELETE request
		   * @param {String} sAccept the HTTPAccept method to use
		   */
		  DELETE:function(sUrl, oRequestData, sAccept){
		      if(!sAccept){
		          sAccept = HTTPAccept.JSON;
		      }
		      this._dial(sUrl, oRequestData, HTTPMethod.DELETE, sAccept);
		  },
		  /**
		   * Call the HEAD method of the given URL using the given JSON as parameter using the given accept-header
		   * @method HEAD
		   * @param {String} sUrl the URL to send the HEAD request to
		   * @param {Object} oRequestData the JSON to use as parameter for this HEAD request
		   * @param {String} sAccept the HTTPAccept method to use
		   */
		  HEAD:function(sUrl, oRequestData, sAccept){
		      if(!sAccept){
		          sAccept = HTTPAccept.JSON;
		      }
		      this._dial(sUrl, oRequestData, HTTPMethod.HEAD, sAccept);
		  },
		  /**
		   * Callback method for responses that actually have a HTTP error code (i.e.: 404).
		   * This method is also called by success method, if there's an error header and the server response is 200.
		   * This method checks to see if there is an onError function in the localData part of the request JSON and applies it
		   * if present, else calling the onError method below.
		   * @method failure
		   * @param {Object} response the response
		   */
		  failure: function(response) {                     
		      if(typeof this.RCS[response.tId].localData.onError==="function"){
		          this.RCS[response.tId].localData.onError.call(this, this.RCS[response.tId], response);
		      } else {
		          this.onError(this.RCS[response.tId],response);
		      }
		      this.finalizeCall(response.tId);
		  },
		  /**(
		   * Callback method for responses that actual(ly worked as they should.
		   * This method checks to see if there is an onSuccess function in the localData part of the request JSON and applies it
		   * if present, else calling the onSuccess method below.
		   * @method success
		   * @param {Object} response the response
		   */
		  success: function(response){
		      if(response["getResponseHeader"]["login"]=="true"){
		          document.location.href="";
		          return;
		      }        
		      if(response["getResponseHeader"]["Error"]=="true"){
		          this.failure(response);
		          return;
		      }
		      if(typeof this.RCS[response.tId].localData.onSuccess==="function"){
		          this.RCS[response.tId].localData.onSuccess.call(this, this.RCS[response.tId], response);
		      } else {
		          this.onSuccess(this.RCS[response.tId],response);
		      }        
		      this.finalizeCall(response.tId);
		  },
		  /**
		   * Finalizes the call by calling addComplete method and after that deleting the oRequest JSON from the container of
		   * request JSONS
		   * @method finalizeCall
		   * @param {String} transactionId
		   */
		  finalizeCall:function(transactionId){
		      this.addComplete(transactionId, this.RCS[transactionId].localData[HTTPCONST.COMPLETEMESSAGE]);
		      delete this.RCS[transactionId];        
		  },
		  /**
		   * A dummy implementation simply alerting the responseText in case an error occurred
		   * @method onError
		   * @param {Object} RC the oRequest JSON
		   * @param {Object} response the response from the server
		   */
		  onError: function(RC, response){
		      alert(YAHOO.lang.JSON.stringify(RC) + " " + response.responseText + " " + response.status + " " + response.statusText);
		  },
		  /**
		   * A dummy implementation simply alerting the responseText in case of success
		   * @method onSuccess
		   * @param {Object} RC the oRequest JSON
		   * @param {Object} response the response from the server
		   */
		  onSuccess: function(RC, response){
		      alert(YAHOO.lang.JSON.stringify(RC) + " " + response.responseText);
		  },
		  /**
		  * Locks the screen with the please wait panel while executing a server call.
		  * This method shows (and renders) the wait dialog on need and adds a waitingForProgress_+transactionId div
		  * for every request.
		  * @method addWaitingFor
		  * @param {String} transactionId . The transactionId
		  */
		  addWaitingFor:function(transactionId) {
		  },
		  /**
		  * Removes the waitingForProgress_+transactionId div from the pleaseWait panel, closing and hiding the panel
		  * if it was the last one
		  * @method addComplete
		  */
		  addComplete:function(transactionId, transactionCompleteMessage) {
		  },
		  /**
		   * the scope of the callback for async requests
		   * @property scope
		   */
		  scope: HttpClient
	};

