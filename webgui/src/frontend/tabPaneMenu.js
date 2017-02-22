'use strict';
var $ = require('jquery'),
    imgs = require('./imgs.js'),
    selectize = require('selectize'),
    utils = require('yasgui-utils');


module.exports = function(yasgui, tab) {
    var $menu = null;
    var $tabPanesParent = null;

    var $btnPost;
    var $btnGet;
    var $acceptSelect;
    var $acceptGraph;
    var $urlArgsDiv;
    var $defaultGraphsDiv;
    var $namedGraphsDiv;
    var initWrapper = function() {
        $menu = $('<nav>', {
            id: 'navmenu_' + tab.persistentOptions.id
        });

        //init panes
        $tabPanesParent = $menu;

        var reqPaneId = 'yasgui_reqConfig_' + tab.persistentOptions.id;
        var $reqPanel = $('<div>', {
            id: reqPaneId,
            role: 'tabpanel',
            class: 'tab-pane requestConfig container-fluid form-inline'
        }).appendTo($tabPanesParent);

        //request method
        var $reqRow = $('<div style="line-height:34px;width:250px;float:left;margin-right:20px">', {
            class: 'form-group'
        }).appendTo($reqPanel);

        $('<label>').addClass('control-label').appendTo($reqRow).append($('<span>').text('Request Method'));
        $btnPost = $('<button>', {
            class: 'btn btn-default ',
            'data-toggle': "button"
        }).text('POST').click(function() {
            $btnPost.addClass('active');
            $btnGet.removeClass('active');
            $btnPost.css("color","#333");
            $btnPost.css("border-color","#adadad");
            $btnPost.css("background-color","#e6e6e6");
            $btnPost.css("background-image","none");
            $btnPost.css("box-shadow","inset 0 3px 5px rgba(0, 0, 0, .125)");

            $btnGet.css("color","#333");
            $btnGet.css("border-color","#ccc");
            $btnGet.css("background-color","#fff");
            $btnGet.css("background-image","none");
            $btnGet.css("box-shadow","none");
            tab.persistentOptions.yasqe.sparql.requestMethod = "POST";
        });

        $btnPost.css("color","#333");
        $btnPost.css("border-color","#adadad");
        $btnPost.css("background-color","#e6e6e6");
        $btnPost.css("background-image","none");
        $btnPost.css("box-shadow","inset 0 3px 5px rgba(0, 0, 0, .125)");

        $btnGet = $('<button>', {
            class: 'btn btn-default',
            'data-toggle': "button"
        }).text('GET').click(function() {
            $btnGet.addClass('active');
            $btnPost.removeClass('active');
            $btnGet.css("color","#333");
            $btnGet.css("border-color","#adadad");
            $btnGet.css("background-color","#e6e6e6");
            $btnGet.css("background-image","none");
            $btnGet.css("box-shadow","inset 0 3px 5px rgba(0, 0, 0, .125)");

            $btnPost.css("color","#333");
            $btnPost.css("border-color","#ccc");
            $btnPost.css("background-color","#fff");
            $btnPost.css("background-image","none");
            $btnPost.css("box-shadow","none");
            tab.persistentOptions.yasqe.sparql.requestMethod = "GET";
            /*$("#get_"+tab.persistentOptions.id).attr("class","btn btn-default active");
            console.log($("#get_"+tab.persistentOptions.id).attr("class"));
            console.log($btnGet.attr("class"));
            tab.persistentOptions.yasqe.sparql.requestMethod = "GET";
            console.log(tab.persistentOptions.yasqe.sparql.requestMethod);*/
        });
        $('<div style="float:right;">', {
            class: 'btn-group',
            role: 'group'
        }).append($btnGet).append($btnPost).appendTo($reqRow);

        //Accept headers
        var $acceptRow = $('<div style="line-height:34px;width:600px;margin-left:20px;">', {
            class: 'form-group'
        }).appendTo($reqPanel);
        $('<label>').addClass('control-label').appendTo($acceptRow).text('Accept Formats');

        $acceptGraph = $('<select>', {
            class: 'acceptHeader'
        })
            .append($("<option>", {
                value: 'text/turtle'
            }).text('Turtle'))
            .append($("<option>", {
                value: 'application/rdf+xml'
            }).text('RDF-XML'))
            .append($("<option>", {
                value: 'text/csv'
            }).text('CSV'))
            .append($("<option>", {
                value: 'text/tab-separated-values'
            }).text('TSV')).appendTo($acceptRow);
        $acceptGraph.selectize({ class: 'form-control', placeholder: "Turtle"});
        //$acceptGraph[0].selectize.setValue("text/turtle");

        $acceptSelect = $('<select>', {
            class: 'acceptHeader'
        })
            .append($("<option>", {
                value: 'application/sparql-results+json'
            }).text('JSON'))
            .append($("<option>", {
                value: 'application/sparql-results+xml'
            }).text('XML'))
            .append($("<option>", {
                value: 'text/csv'
            }).text('CSV'))
            .append($("<option>", {
                value: 'text/tab-separated-values'
            }).text('TSV')).appendTo($acceptRow);
        $acceptSelect.selectize({ class: 'form-control', placeholder: "JSON"});

        //URL args headers
        var $urlArgsRow = $('<div style="margin-top:20px;float:left;">', {
            class: 'form-group'
        }).appendTo($reqPanel);
        $('<label>').appendTo($urlArgsRow).text('URL Arguments');
        $urlArgsDiv = $('<div>', {
            role: 'group',
            class: 'form-group'
        }).appendTo($urlArgsRow);


        //Default graphs
        var $defaultGraphsRow = $('<div style="margin-top:20px;float:left;">', {
            class: 'form-group'
        }).appendTo($reqPanel);
        $('<label>').addClass('control-label').appendTo($defaultGraphsRow).text('Default graphs');
        $defaultGraphsDiv = $('<div>', {
            role: 'group',
            class: 'form-group'
        }).appendTo($defaultGraphsRow);


        //Named graphs
        var $namedGraphsRow = $('<div style="margin-top:20px;float:left;">', {
            class: 'form-group'
        }).appendTo($reqPanel);
        $('<label>').addClass('control-label').appendTo($namedGraphsRow).text('Named graphs');
        $namedGraphsDiv = $('<div>', {
            role: 'group',
            class: 'form-group'
        }).appendTo($namedGraphsRow);

        return $menu;
    };

    var addTextInputsTo = function($el, num, animate, vals) {
        var $inputsAndTogglesContainer = $('<div>', {
            class: 'textInputsRow'
        });
        for (var i = 0; i < num; i++) {
            var val = (vals && vals[i] ? vals[i] : "");
            $('<input>', {
                type: 'text',
                class: 'form-control'
            })
                .val(val)
                .keyup(function() {
                    var lastHasContent = false;
                    $el.find('.textInputsRow:last input').each(function(i, input) {
                        if ($(input).val().trim().length > 0) lastHasContent = true;
                    });
                    if (lastHasContent) {
                        addTextInputsTo($el, num, true);
                        $inputsAndTogglesContainer.append(
                            $('<button>', {
                                class: "close ",
                                type: "button"
                            })
                                .text('x')
                                .click(function() {
                                    $(this).closest('.textInputsRow').remove();
                                })
                            //              $(utils.svg.getElement(imgs.cross, {width: '14px', height: '14px'}))
                            //              .addClass('closeBtn')
                            //              .css('display', '')//let our style sheets do the work here
                            //              .click(function(){
                            //                  $(this).closest('.textInputsRow').remove();
                            //              })
                        );
                    }
                })
                .css('width', (92 / num) + '%')
                .appendTo($inputsAndTogglesContainer);
        }
       /* $inputsAndTogglesContainer.append(
            $('<button>', {
                class: "close ",
                type: "button"
            })
                .text('x')
                .click(function() {
                    $(this).closest('.textInputsRow').remove();
                })
            //				$(utils.svg.getElement(imgs.cross, {width: '14px', height: '14px'}))
            //				.addClass('closeBtn')
            //				.css('display', '')//let our style sheets do the work here
            //				.click(function(){
            //					$(this).closest('.textInputsRow').remove();
            //				})
        );*/
        if (animate) {
            $inputsAndTogglesContainer.hide().appendTo($el).show('fast');
        } else {
            $inputsAndTogglesContainer.appendTo($el);
        }
    };

    var updateWrapper = function() {
        /**
         * update request tab
         */
        //we got most of the html. Now set the values in the html
        var options = tab.persistentOptions.yasqe;


        //Request method
        /*if (options.sparql.requestMethod.toUpperCase() == "POST") {
            $btnPost.addClass('active');
        } else {
            $btnGet.addClass('active');
        }*/
        //Request method
        $acceptGraph[0].selectize.setValue(options.sparql.acceptHeaderGraph);
        $acceptSelect[0].selectize.setValue(options.sparql.acceptHeaderSelect);

        //url args
        $urlArgsDiv.empty();
        if (options.sparql.args && options.sparql.args.length > 0) {
            options.sparql.args.forEach(function(el) {
                var vals = [el.name, el.value];
                addTextInputsTo($urlArgsDiv, 2, false, vals)
            });
        }
        addTextInputsTo($urlArgsDiv, 2, false); //and, always add one item


        //default graphs
        $defaultGraphsDiv.empty();
        if (options.sparql.defaultGraphs && options.sparql.defaultGraphs.length > 0) {
            addTextInputsTo($defaultGraphsDiv, 1, false, options.sparql.defaultGraphs)
        }
        addTextInputsTo($defaultGraphsDiv, 1, false); //and, always add one item

        //default graphs
        $namedGraphsDiv.empty();
        if (options.sparql.namedGraphs && options.sparql.namedGraphs.length > 0) {
            addTextInputsTo($namedGraphsDiv, 1, false, options.sparql.namedGraphs)
        }
        addTextInputsTo($namedGraphsDiv, 1, false); //and, always add one item

    };

    var store = function() {
        console.log("store");
        var options = tab.persistentOptions.yasqe.sparql;
        if ($btnPost.hasClass('active')) {
            options.requestMethod = "POST";
        } else if ($btnGet.hasClass('active')) {
            options.requestMethod = "GET";
        }

        console.log(options.requestMethod);

        //Request method
        //options.acceptHeaderGraph = $acceptGraph[0].selectize.getValue();
        //options.acceptHeaderSelect = $acceptSelect[0].selectize.getValue();
        //options.acceptHeaderGraph = $acceptGraph.find('option:selected').text();
        //options.acceptHeaderSelect = $acceptSelect.find('option:selected').text();
        //console.log($acceptSelect.find('option:selected').val().text());

        //This part of the code is WRONG and UNNECESSARY, because we can use .val() and get the value of the selected option of the dropdown list.
        //.val() returns "" (empty string) for some unknown reason though, so it's useless.
        if($acceptGraph.find('option:selected').text() === "Turtle"){
            options.acceptHeaderGraph = "text/turtle";
        }
        else if($acceptGraph.find('option:selected').text() === "RDF-XML"){
            options.acceptHeaderGraph = "application/rdf+xml"
        }
        else if($acceptGraph.find('option:selected').text() === "CSV"){
            options.acceptHeaderGraph = "text/csv"
        }
        else if($acceptGraph.find('option:selected').text() === "TSV"){
            options.acceptHeaderGraph = "text/tab-separated-values"
        }

        if($acceptSelect.find('option:selected').text() === "JSON"){
            options.acceptHeaderSelect = "application/sparql-results+json";
        }
        else if($acceptSelect.find('option:selected').text() === "XML"){
            options.acceptHeaderSelect = "application/sparql-results+xml"
        }
        else if($acceptSelect.find('option:selected').text() === "CSV"){
            options.acceptHeaderSelect = "text/csv"
        }
        else if($acceptSelect.find('option:selected').text() === "TSV"){
            options.acceptHeaderSelect = "text/tab-separated-values"
        }
        console.log(options.acceptHeaderSelect);

        //url args
        var args = [];
        $urlArgsDiv.find('div').each(function(i, el) {
            var inputVals = [];
            $(el).find('input').each(function(i, input) {
                inputVals.push($(input).val());
            });
            if (inputVals[0] && inputVals[0].trim().length > 0) {
                args.push({
                    name: inputVals[0],
                    value: (inputVals[1] ? inputVals[1] : "")
                });
            }
        });
        options.args = args;


        //default graphs
        var defaultGraphs = [];
        $defaultGraphsDiv.find('div').each(function(i, el) {
            var inputVals = [];
            $(el).find('input').each(function(i, input) {
                inputVals.push($(input).val());
            });
            if (inputVals[0] && inputVals[0].trim().length > 0) defaultGraphs.push(inputVals[0]);
        });
        options.defaultGraphs = defaultGraphs;

        //named graphs
        var namedGraphs = [];
        $namedGraphsDiv.find('div').each(function(i, el) {
            var inputVals = [];
            $(el).find('input').each(function(i, input) {
                inputVals.push($(input).val());
            });
            if (inputVals[0] && inputVals[0].trim().length > 0) namedGraphs.push(inputVals[0]);
        });
        options.namedGraphs = namedGraphs;
        yasgui.store();
        tab.setPersistentInYasqe();
    };



    return {
        initWrapper: initWrapper,
        updateWrapper: updateWrapper,
        store: store
    };
};
