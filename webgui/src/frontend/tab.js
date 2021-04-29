'use strict';

//		mod.emit('initError')
//		mod.once('initDone', load);


var $ = require('jquery'),
    EventEmitter = require('events').EventEmitter,
    utils = require('./utils.js'),
    yUtils = require('yasgui-utils'),
    _ = require('underscore'),
    YASGUI = require('./main.js'),
    bootstrap = require('bootstrap');
//we only generate the settings for YASQE, as we modify lots of YASQE settings via the YASGUI interface
//We leave YASR to store its settings separately, as this is all handled directly from the YASR controls

var defaultPersistent = {
    yasqe: {
        height: 250,
        createShareLink: null,
        consumeShareLink: null,
        sparql: {
            endpoint: YASGUI.YASQE.defaults.sparql.endpoint,
            acceptHeaderGraph: YASGUI.YASQE.defaults.sparql.acceptHeaderGraph,
            acceptHeaderSelect: YASGUI.YASQE.defaults.sparql.acceptHeaderSelect,
            args: YASGUI.YASQE.defaults.sparql.args,
            defaultGraphs: YASGUI.YASQE.defaults.sparql.defaultGraphs,
            namedGraphs: YASGUI.YASQE.defaults.sparql.namedGraphs,
            requestMethod: YASGUI.YASQE.defaults.sparql.requestMethod,
            showQueryButton: true
        }
    }
};



module.exports = function(yasgui, id, name, endpoint) {
    return new Tab(yasgui, id, name, endpoint);
}
var Tab = function(yasgui, id, name, endpoint) {
    EventEmitter.call(this);
    if (!yasgui.persistentOptions.tabs[id]) {
        yasgui.persistentOptions.tabs[id] = $.extend(true, {
            id: id,
            name: name
        }, defaultPersistent);
    } else {
        yasgui.persistentOptions.tabs[id] = $.extend(true, {}, defaultPersistent, yasgui.persistentOptions.tabs[id]);
    }
    var persistentOptions = yasgui.persistentOptions.tabs[id];
    if (endpoint) persistentOptions.yasqe.sparql.endpoint = endpoint;
    var tab = this;
    tab.persistentOptions = persistentOptions;

    var menu = require('./tabPaneMenu.js')(yasgui, tab);
    var $pane = $('<div>', {
        id: persistentOptions.id,
        style: 'position:relative',
        class: 'tab-pane',
        role: 'tabpanel'
    }).appendTo(yasgui.$tabPanesParent);

    var $paneContent = $('<div>', {
        class: 'wrapper'
    }).appendTo($pane);
    var $controlBar = $('<div>', {
        class: 'controlbar'
    }).appendTo($paneContent);
    var $endpointInput;
    var $queryBtn;
    var $decomposeBtn;
    //var $monitorBtn;
    var addControlBar = function() {

        var $btnGroup = $('<div>', {
            class: 'btn-group'
        }).appendTo($controlBar);



        var $button = $('<button>', {
            type: 'button',
            class: 'menuButton btn btn-default'
            })
            .append($('<span>', {
                class: 'icon-bar'
            }))
            .append($('<span>', {
                class: 'icon-bar'
            }))
            .append($('<span>', {
                class: 'icon-bar'
            }))
            .click()
            .appendTo($btnGroup);


        $decomposeBtn = $('<button>', {
            type: 'button',
            class: 'menuButton btn btn-default'
        })
            .text("Decompose")
            .appendTo($btnGroup);

        $queryBtn = $('<button>', {
            type: 'button',
            class: 'menuButton btn btn-default'
        })
            .text("Execute")
            .appendTo($btnGroup);

	/*
        $monitorBtn = $('<button>', {
            type: 'button',
            class: 'menuButton btn btn-default'
        })
            .text("Monitor")
            .appendTo($btnGroup);

        $monitorBtn.hide();
	*/

        var $paneMenu = menu.initWrapper().appendTo($controlBar);
            menu.updateWrapper();
            $paneMenu.collapse({toggle:false});
            $paneMenu.addClass('collapse');
            $button.click(function() { $paneMenu.collapse("toggle"); });
    };

    var changeEndpoint = function(val) {
        persistentOptions.yasqe.sparql.endpoint = val;
        tab.refreshYasqe();
        yasgui.store();
    }

    var yasqeContainer = $('<div>', {
        id: 'yasqe_' + persistentOptions.id
    }).appendTo($paneContent);
    /*
    var monitorContainer = $('<iframe/>', {
        id: 'monitor_' + persistentOptions.id,
        src: "./resources/monitor/index.html",
        style: "width:100%; height:350px; border:0; display:none;"
    }).appendTo($paneContent);
    */
    var treeContainer = $('<iframe/>', {
        id: 'tree_' + persistentOptions.id,
        src: "./resources/semagrow_site/index.html",
        style: "width:100%; height:725px; border:0; display:none;"
        //scrolling: "no"
    }).appendTo($paneContent);
    var yasrContainer = $('<div>', {
        id: 'yasq_' + persistentOptions.id
    }).appendTo($paneContent);



    var storeInHist = function() {
        persistentOptions.yasqe.value = tab.yasqe.getValue(); //in case the onblur hasnt happened yet
        var resultSize = null;
        if (tab.yasr.results.getBindings()) {
            resultSize = tab.yasr.results.getBindings().length;
        }
        var histObject = {
            options: $.extend(true, {}, persistentOptions), //create copy
            resultSize: resultSize
        };
        delete histObject.options.name; //don't store this one
        yasgui.history.unshift(histObject);

        var maxHistSize = 50;
        if (yasgui.history.length > maxHistSize) {
            yasgui.history = yasgui.history.slice(0, maxHistSize);
        }


        //store in localstorage as well
        if (yasgui.persistencyPrefix) {
            yUtils.storage.set(yasgui.persistencyPrefix + 'history', yasgui.history);
        }


    };

    tab.setPersistentInYasqe = function() {
        if (tab.yasqe) {
            $.extend(tab.yasqe.options.sparql, persistentOptions.yasqe.sparql);
            //set value manualy, as this triggers a refresh
            if (persistentOptions.yasqe.value) tab.yasqe.setValue(persistentOptions.yasqe.value);
        }
    }

    var yasqeOptions = {};

    var yasrDisplayed = false;
    var decomposeDisplayed = false;
    $.extend(yasqeOptions, persistentOptions.yasqe);

    var initYasr = function() {
        if (!tab.yasr) {
            var addQueryDuration = function(yasr, plugin) {
                if (tab.yasqe.lastQueryDuration && plugin.name == "Table") {
                    var tableInfo = tab.yasr.resultsContainer.find('.dataTables_info');
                    if (tableInfo.length > 0) {
                        var text = tableInfo.first().text();
                        tableInfo.text(text + ' (in ' + (tab.yasqe.lastQueryDuration / 1000) + ' seconds)');
                    }
                }
            }
            if (!tab.yasqe) initYasqe(); //we need this one to initialize yasr
            var getQueryString = function() {
                return persistentOptions.yasqe.sparql.endpoint + "?" +
                    $.param(tab.yasqe.getUrlArguments(persistentOptions.yasqe.sparql));
            };
            YASGUI.YASR.plugins.error.defaults.tryQueryLink = getQueryString;
            tab.yasr = YASGUI.YASR(yasrContainer[0], $.extend({
                outputPlugins: ["error", "boolean", "rawResponse", "table"],
                //this way, the URLs in the results are prettified using the defined prefixes in the query
                getUsedPrefixes: tab.yasqe.getPrefixesFromQuery
            }, persistentOptions.yasr));
            tab.yasr.on('drawn', addQueryDuration);
        }
        if(!yasrDisplayed){
            document.getElementById('yasq_' + persistentOptions.id).style.display="none";
        }
        $(".select_pivot").css("display", "none");
        $(".select_gchart").css("display", "none");

    };
    tab.query = function() {
        tab.yasqe.query();
    };


    var initYasqe = function() {
        if (!tab.yasqe) {
            YASGUI.YASQE.defaults.extraKeys['Ctrl-Enter'] = function() {
                tab.yasqe.query.apply(this, arguments)
            };
            YASGUI.YASQE.defaults.extraKeys['Cmd-Enter'] = function() {
                tab.yasqe.query.apply(this, arguments)
            };
            tab.yasqe = YASGUI.YASQE(yasqeContainer[0], yasqeOptions);

            addControlBar();


            $(".yasqe_queryButton").css("display", "none");
            tab.yasqe.setSize("100%", persistentOptions.yasqe.height);
            tab.yasqe.on('blur', function(yasqe) {
                persistentOptions.yasqe.value = yasqe.getValue();
                yasgui.store();
            });
            tab.yasqe.on('query', function() {

                //yasgui.$tabsParent.find('a[href="#' + id + '"]').closest('li').addClass('querying');
                $('.nav-tabs .active').addClass('querying');
                //yasgui.emit('query', yasgui, tab);
                //tab.emit('query')
            });
            tab.yasqe.on('queryFinish', function() {
                //yasgui.$tabsParent.find('a[href="#' + id + '"]').closest('li').removeClass('querying');
                $('.nav-tabs .active').removeClass('querying');
                yasgui.emit('queryFinish', yasgui, tab);
                tab.emit('queryFinish');
                //$monitorBtn.show();
            });
            var beforeSend = null;
            tab.yasqe.options.sparql.callbacks.beforeSend = function() {
                beforeSend = +new Date();
            }
            tab.yasqe.options.sparql.callbacks.complete = function() {
                var end = +new Date();
                tab.yasr.setResponse.apply(this, arguments);
                storeInHist();
            }

            tab.yasqe.query = function() {
                var options = {}
                options = $.extend(true, options, tab.yasqe.options.sparql);
                if (yasgui.options.api.corsProxy && yasgui.corsEnabled) {
                    if (!yasgui.corsEnabled[persistentOptions.yasqe.sparql.endpoint]) {
                        //use the proxy //name value

                        options.args.push({
                            name: 'endpoint',
                            value: options.endpoint
                        });
                        options.args.push({
                            name: 'requestMethod',
                            value: options.requestMethod
                        });
                        options.requestMethod = "POST";
                        options.endpoint = yasgui.options.api.corsProxy;
                        YASGUI.YASQE.executeQuery(tab.yasqe, options);
                    } else {
                        YASGUI.YASQE.executeQuery(tab.yasqe, options);
                    }
                } else {
                    YASGUI.YASQE.executeQuery(tab.yasqe, options);
                }
            };

            $queryBtn.click(function(){
                //document.getElementById('yasq_' + persistentOptions.id).style.display="block";
                treeContainer.css("display","none");
                yasrContainer.css("display","block");
                //$monitorBtn.hide();
                yasrDisplayed = true;
                decomposeDisplayed = false;
                //$(".yasqe_queryButton").click();
                menu.store()
                tab.yasqe.query();
                //console.log(tab.yasqe.options.sparql);
            });

            $decomposeBtn.click(function(){
                yasrContainer.css("display","none");//document.getElementById('yasq_' + persistentOptions.id).style.display="none";
                treeContainer.css("display","block");
                yasrDisplayed = false;
                decomposeDisplayed = true;
                //var h = document.getElementById('tree_' + persistentOptions.id).contentWindow.document.body.scrollHeight;
                //document.getElementById('tree_' + persistentOptions.id).height=h;
                document.getElementById('tree_' + persistentOptions.id).contentWindow.init("../../sparql",tab.yasqe);
                document.getElementById('tree_' + persistentOptions.id).contentWindow.raw_();
                //console.log(document.getElementById('tree_' + persistentOptions.id).contentWindow.document.getElementById("result2").offsetHeight);
                //document.getElementById('tree_' + persistentOptions.id).height=document.getElementById('tree_' + persistentOptions.id).contentWindow.document.getElementById("result2").offsetHeight;
            });

	    /*
            $monitorBtn.click(function(){
                //yasrContainer.css("display","none");//document.getElementById('yasq_' + persistentOptions.id).style.display="none";
                if(monitorContainer.css("display") == "none"){
                    monitorContainer.css("display","block");
                }
                else{
                    monitorContainer.css("display", "none");
                }
                //yasrDisplayed = false;
                //decomposeDisplayed = true;
                //var h = document.getElementById('tree_' + persistentOptions.id).contentWindow.document.body.scrollHeight;
                //document.getElementById('tree_' + persistentOptions.id).height=h;
                //document.getElementById('monitor_' + persistentOptions.id).contentWindow.init("../../sparql",tab.yasqe);
                //document.getElementById('monitor_' + persistentOptions.id).contentWindow.raw_();
                //console.log(document.getElementById('tree_' + persistentOptions.id).contentWindow.document.getElementById("result2").offsetHeight);
                //document.getElementById('tree_' + persistentOptions.id).height=document.getElementById('tree_' + persistentOptions.id).contentWindow.document.getElementById("result2").offsetHeight;
            });
            */
            tab.yasqe.on('query', function() { 
                $queryBtn.css("background-color","white");
                $queryBtn.css("color","black");
                $queryBtn.text("Running...Cancel"); 
            });
            tab.yasqe.on('queryFinish', function() { 
                $queryBtn.text("Execute").addClass('btn-success'); 
                $queryBtn.css("background-color","#5cb85c");
            });
        }
    };


    tab.onShow = function() {
        initYasqe();
        tab.yasqe.refresh();
        initYasr();
        if (yasgui.options.allowYasqeResize) {
            $(tab.yasqe.getWrapperElement()).resizable({
                minHeight: 150,
                handles: 's',
                resize: function() {
                    _.debounce(function() {
                        tab.yasqe.setSize("100%", $(this).height());
                        tab.yasqe.refresh()
                    }, 500);
                },
                stop: function() {
                    persistentOptions.yasqe.height = $(this).height();
                    tab.yasqe.refresh()
                    yasgui.store();
                }
            });
            $(tab.yasqe.getWrapperElement()).find('.ui-resizable-s').click(function() {
                $(tab.yasqe.getWrapperElement()).css('height', 'auto');
                persistentOptions.yasqe.height = 'auto';
                yasgui.store();
            })
        }
    };

    tab.beforeShow = function() {
        initYasqe();
    }
    tab.refreshYasqe = function() {
        if (tab.yasqe) {
            $.extend(true, tab.yasqe.options, tab.persistentOptions.yasqe);
            if (tab.persistentOptions.yasqe.value) tab.yasqe.setValue(tab.persistentOptions.yasqe.value);
        }
    };
    tab.destroy = function() {
        if (!tab.yasr) {
            //instantiate yasr (without rendering results, to avoid load)
            //this way, we can clear the yasr persistent results
            tab.yasr = YASGUI.YASR(yasrContainer[0], {
                outputPlugins:  ["error", "boolean", "rawResponse", "table"]
            }, '');
        }
        yUtils.storage.removeAll(function(key, val) {
            return key.indexOf(tab.yasr.getPersistencyId('')) == 0;
        })
    }
    tab.getEndpoint = function() {
        var endpoint = null;
        if (yUtils.nestedExists(tab.persistentOptions, 'yasqe', 'sparql', 'endpoint')) {
            endpoint = tab.persistentOptions.yasqe.sparql.endpoint;
        }
        return endpoint;
    }

    return tab;
}

Tab.prototype = new EventEmitter;
