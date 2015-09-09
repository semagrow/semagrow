<%@page contentType="text/html" pageEncoding="UTF-8"%>
<% if(request.getUserPrincipal()==null){ %>
<span class="form-inline">
<label for="user">User: </label><input type="text" placeholder="Username" class="form-control"  id="user"/>
<label for="pass">Pass: </label><input placeholder="Password" class="form-control" type="password" id="pass"/>
<button type="button" class="btn btn-default" onclick="javascript:SemaGrowPage.authenticate()" >login</button
</span>
<% } else { %>
<p>logged in as <strong><%=request.getUserPrincipal().getName()%></strong> <a href="javascript:SemaGrowPage.logout();">logout</a></p>
<% } %>