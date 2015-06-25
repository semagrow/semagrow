
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
    "http://www.w3.org/TR/html4/loose.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>SemaGrow-Stack</title>
        <link rel="stylesheet" type="text/css" href="resources/styles/style.css" />
        <link rel="stylesheet" type="text/css" href="resources/styles/tabview.css" />
        <link rel="stylesheet" type="text/css" href="resources/styles/datatable.css" />
        <link rel="stylesheet" type="text/css" href="http://yui.yahooapis.com/2.9.0/build/tabview/assets/skins/sam/tabview.css">
        <link rel="stylesheet" type="text/css" href="http://yui.yahooapis.com/2.9.0/build/datatable/assets/skins/sam/datatable.css" />
        <script src="http://yui.yahooapis.com/2.9.0/build/yahoo/yahoo-min.js" ></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/event/event-min.js" ></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/yahoo-dom-event/yahoo-dom-event.js"></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/datasource/datasource-min.js"></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/connection/connection_core-min.js"></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/connection/connection-min.js"></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/yahoo/yahoo-min.js"></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/element/element-min.js"></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/tabview/tabview-min.js"></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/event-delegate/event-delegate-min.js"></script>
        <script src="http://yui.yahooapis.com/2.9.0/build/datatable/datatable-min.js"></script>        
        <script src="resources/js/CONSTANTS.js"></script>
        <script src="resources/js/openrdf/model/Model.js"></script>
        <script src="resources/js/openrdf/vocabulary/VOCAB.js"></script>
        <script src="resources/js/ApplicationState.js"></script>
        <script src="resources/js/http/HttpClient-0.0.1.js"></script>
        <script src="resources/js/SemaGrowPage.js"></script>
        <script>
            var app;
            function init() {
                var customEvents = new Array();
                customEvents.push(CONSTANTS.EVENTS.CURRENT_ENTITY_INFO);

                app = ApplicationState.newInstance(customEvents);
                app.setKeyListener(CONSTANTS.EVENTS.CURRENT_ENTITY_INFO, SemaGrowTabs.createTabs, SemaGrowTabs);
                app.set(CONSTANTS.EVENTS.CURRENT_ENTITY_INFO,SIOC.SPACE,false);
            }
        </script>
        <style>
            
        </style>
    </head>
    <body class="yui-skin-sam">
        <div id="header">
            <img id="logo" src="resources/images/logo.png"/>
            <div id="auth">
                <jsp:include page="/WEB-INF/jsp/auth/login.jsp" />
            </div>
        </div>
        <div id="content"></div>
        <div id="footer"></div>
    </body>
    <script>
        if (document.addEventListener) {
            document.addEventListener("DOMContentLoaded", init, false);
        } else {
            window.onload = init;
        }
    </script>    
</html>