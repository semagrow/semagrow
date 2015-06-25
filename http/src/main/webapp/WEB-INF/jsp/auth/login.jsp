<%@page contentType="text/html" pageEncoding="UTF-8"%>
<% if(request.getUserPrincipal()==null){ %>
<label for="user">User: </label><input type="text" id="user"/>
<label for="pass">Pass: </label><input type="password" id="pass"/>
<button type="button" onclick="javascript:SemaGrowPage.authenticate()">login</button>
<% } else { %>
logged in as <strong><%=request.getUserPrincipal().getName()%></strong> <a href="javascript:SemaGrowPage.logout();">logout</a>
<% } %>