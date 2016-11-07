/**
 *
 * @author http://www.turnguard.com/turnguard
 */
SemaGrowUtils = {
    StringUtils: {
        replaceAngleBrackets: function(s){
            return s.replace(/</g, function(c) { return '&lt;'; })
                    .replace(/>/g,function(c){ return '&gt;';});   
        }
    }    
};
SemaGrowPage = {
    onLoad:function(type, args, scope){
        alert("onLoad " + type + " " + args + " " + scope);
    },
    loadWelcome: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("welcomeContent").innerHTML=response.responseText;
                }
            }
        };
        HttpClient.GET("welcome", oRequestData, HTTPAccept.HTML);         
    },
    loadVocBench: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("vocBenchContent").innerHTML=response.responseText;
                }
            }
        };
        HttpClient.GET("page?template=vocbench", oRequestData, HTTPAccept.HTML);         
    },
    loadAdminPage: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("adminContent").innerHTML=response.responseText;
                }
            }
        };
        HttpClient.GET("page?template=admin", oRequestData, HTTPAccept.HTML);         
    },
    loadDocsPage: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("docsContent").innerHTML=response.responseText;
                }
            }
        };
        HttpClient.GET("page?template=docs", oRequestData, HTTPAccept.HTML);         
    },
    loadGettingStarted: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("gettingStartedContent").innerHTML=response.responseText;
                }
            }
        };
        HttpClient.GET("page?template=gettingstarted", oRequestData, HTTPAccept.HTML);         
    },
    loadYASGUI: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("sparqlContent").innerHTML=response.responseText;
                    var yasgui = YASGUI(document.getElementById("sparqlContainer"), {
                        endpoint: "sparql",

                    });
                }
            }
        };
        HttpClient.GET("page?template=yasgui", oRequestData, HTTPAccept.HTML);
    },
    login: function(){       
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("auth").innerHTML=response.responseText;
                }
            }
        };           
        HttpClient.GET("auth/login", oRequestData, HTTPAccept.HTML);
    },
    logout: function(){       
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("auth").innerHTML=response.responseText;
                },
                onError: function(oRequestData, response){
                    switch(response.status){
                        case 401:
                            SemaGrowPage.login();
                        break;
                    }
                }
            }
        };           
        HttpClient.GET("auth/logout", oRequestData, HTTPAccept.HTML);
    }, 
    reloadConfig: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    alert("reloadConfig: status: " + response.status + " accepted, successfully reloaded config");
                    SemaGrowPage.loadAdminPage();
                },
                onError: function(oRequestData, response){
                    switch(response.status){
                        case 401:
                            alert("reloadConfig: unauthorized: " + response.status + ", user role does not permit reloading config");
                        break;
                        case 403:
                            alert("reloadConfig: forbidden: " + response.status);
                        break;                        
                    }
                    SemaGrowPage.loadAdminPage();                    
                }
            }
        };           
        HttpClient.GET("sparql/reloadConfig", oRequestData, HTTPAccept.HTML);        
    },
    authenticate: function(){       
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("auth").innerHTML=response.responseText;
                },
                onError:function(oRequestData, response){
                    alert("error");
                }
            }
        };
        var currentUrl = document.location.href.replace("http://","");
        var authUrl = "http://"+document.getElementById("user").value+":"+document.getElementById("pass").value+"@"+currentUrl+"auth/authenticate";
        HttpClient.GET(authUrl, oRequestData, HTTPAccept.HTML);
    }    
};

SemaGrowTabs = {
    createTabs:function(type, args, scope){  
        switch(args[1]){
            case SIOC.SPACE:
                scope.createSiocSpaceTabs();
            break;
        }
    },
    createSiocSpaceTabs: function(){
        /* unset tabview if defined */
        if(this.mainTabs!==undefined){
            this.mainTabs.destroy();
        }
        
        /* create tabview */
        this.mainTabs = new YAHOO.widget.TabView();
        
        /* define tabs */
        this.mainTabs.addTab( new YAHOO.widget.Tab({
            label: 'Getting Started',
            id: 'gettingstarted',
            content: '<div id=\"gettingStartedContent\"></div>',
            cacheData: true
        }));
        this.mainTabs.addTab( new YAHOO.widget.Tab({
            label: 'Sparql',
            id: 'sparql',
            content: '<div id=\"sparqlContent\"></div>',
            cacheData: true
        }));
        /*
        this.mainTabs.addTab( new YAHOO.widget.Tab({
            label: 'Federation',
            id: 'federation',
            content: '<div id=\"federationContent\"></div>',
            cacheData: true
        }));  
        */
        this.mainTabs.addTab( new YAHOO.widget.Tab({
            label: 'Docs',
            id: 'docs',
            content: '<div id=\"docsContent\"></div>',
            cacheData: true
        }));       
        this.mainTabs.addTab( new YAHOO.widget.Tab({
            label: 'Admin',
            id: 'admin',
            content: '<div id=\"adminContent\"></div>',
            cacheData: true
        }));
        this.mainTabs.addTab( new YAHOO.widget.Tab({
            label: 'VocBench',
            id: 'vocbench',
            content: '<div id=\"vocBenchContent\"></div>',
            cacheData: true
        }));        
        
        /* append tabview to content */
        this.mainTabs.appendTo("content");

        $('.yui-navset ul')
            .removeClass('yui-nav')
            .addClass('nav navbar navbar-nav navbar-default')
            .css({"display": "block", "width":"100%", "margin":"2px"});

        $('.yui-content').css({"clear":"both", "background-color": "white"});
        
        /* tab change routine */
        this.mainTabs.on('activeTabChange', function(ev) {
            switch(ev.newValue.get("id")){
                case 'sparql':
                    SemaGrowPage.loadYASGUI();
                break;
                case 'gettingstarted':
                    SemaGrowPage.loadGettingStarted();
                break;
                case 'vocbench':
                    SemaGrowPage.loadVocBench();
                break;
                case 'admin':
                    SemaGrowPage.loadAdminPage();
                break; 
                case 'docs':
                    SemaGrowPage.loadDocsPage();
                break;             
            }
        });
        
        /* activating first tab */
        this.mainTabs.set('activeIndex', 1);
    }
};

SemaGrowSparql = {
    loadSparql:function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("sparqlContent").innerHTML=response.responseText;
                    SemaGrowSparql.loadSparqlSamples();
                }
            }
        };                    
        HttpClient.GET("sparql",oRequestData, HTTPAccept.HTML);         
    },
    loadSparqlSamples:function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("sparqlSamplesContainer").innerHTML=response.responseText;
                    var columnDefs = [
                                {key:"Title",label:"Title",sortable:true},
                                {key:"Description",label:"Description", sortable:true},
                                {key:"SPARQL Query",label:"SPARQL Query",sortable:true}                                
                            ];
                    var dataSource = new YAHOO.util.DataSource(YAHOO.util.Dom.get("sparqlSamples"));
                        dataSource.responseType = YAHOO.util.DataSource.TYPE_HTMLTABLE; 
                        dataSource.responseSchema = {
                                    fields: [{key:"Title"},
                                            {key:"Description"},
                                            {key:"SPARQL Query"}
                                    ]
                                };                        
                    var dataTable = new YAHOO.widget.DataTable("sparqlSamplesContainer", columnDefs, dataSource,
                                    {
                                        caption:"Sample SPARQL Queries",
                                        width: "500px",
                                        sortedBy:{key:"Title",dir:"asc"}
                                    }
                                    );  
                        dataTable.subscribe("rowClickEvent", function(e) {
                            var target = e.target,
                            record = this.getRecord(target);
                            YAHOO.util.Dom.get("query").value=record.getData("SPARQL Query").replace(/&gt;/g,'>').replace(/&lt;/g,'<');
                        });                            
                }
            }
        };                    
        HttpClient.GET("samples",oRequestData, HTTPAccept.HTML);                 
    },
    explainSparqlQuery: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){                    
                    document.getElementById("sparqlResponse").innerHTML="<pre>"+response.responseText+"</pre>";
                },
                onError: function(oRequestData, response){
                    document.getElementById("sparqlResponse").innerHTML=response.responseText;
                },
                "form":"sparqlQuery"
            }
        };
        HttpClient.POST("sparql/explain", oRequestData, HTTPAccept.HTML);         
    },
    explainDecomposedQuery: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){
                    document.getElementById("sparqlResponse").innerHTML="<pre>"+response.responseText+"</pre>";
                },
                onError: function(oRequestData, response){
                    document.getElementById("sparqlResponse").innerHTML=response.responseText;
                },
                "form":"sparqlQuery"
            }
        };
        HttpClient.POST("sparql/decompose", oRequestData, HTTPAccept.HTML);
    },
    runSparqlQuery: function(){
        var oRequestData = {
            "localData":{
                "onSuccess":function(oRequestData, response){                    
                    if(response.status===200){
                        var accept = response.getResponseHeader["Content-Type"];
                        document.getElementById("accept").value = accept;
                        if(accept=="text/html"){ 
                            document.getElementById("sparqlResponse").innerHTML=response.responseText;
                        } else {
                            document.getElementById("sparqlResponse").innerHTML="<pre>"+
                                    SemaGrowUtils.StringUtils.replaceAngleBrackets(response.responseText)+"</pre>"; 
                        }
                    }
                    if(response.status===204){
                        document.getElementById("sparqlResponse").innerHTML="HTTP 204 - indicating success.";
                    }
                },
                onError: function(oRequestData, response){
                    switch(response.status){
                        case 403:
                            document.getElementById("sparqlResponse").innerHTML = "HTTP 403 - You need to login.";
                        break;
                        case 401:
                            document.getElementById("sparqlResponse").innerHTML = "HTTP 401 - Your roles do not permit UPDATES/DELETES.";
                        break;
                        case 500:
                            document.getElementById("sparqlResponse").innerHTML = "HTTP 500 - Something went wrong.<br/>"+response.responseText;
                        break;                        
                    }                  
                },
                "form":"sparqlQuery"
            }
        };
        HttpClient.POST("sparql", oRequestData, HTTPAccept.HTML);         
    }    
};

