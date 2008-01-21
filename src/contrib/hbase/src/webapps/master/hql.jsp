<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.hql.TableFormatter"
  import="org.apache.hadoop.hbase.hql.ReturnMsg"
  import="org.apache.hadoop.hbase.hql.generated.HQLParser"
  import="org.apache.hadoop.hbase.hql.Command"
  import="org.apache.hadoop.hbase.hql.formatter.HtmlTableFormatter" 
%><?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" 
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd"> 
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>HQL</title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>

<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo_med.gif" alt="Hbase Logo" title="Hbase Logo" /></a>
<h1 id="page_title"><a href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseShell">HQL</a></h1>
<p id="links_menu"><a href="/master.jsp">Home</a></p>
<hr id="head_rule" />
<% String query = request.getParameter("q");
   if (query == null) {
     query = "";
   }
%>
<form action="/hql.jsp" method="get">
    <p>
    <label for="query">Query: </label>
    <input type="text" name="q" id="q" size="60" value="<%= query %>" />
    <input type="submit" value="submit" />
    </p>
 </form>
 <p>Enter 'help;' -- thats 'help' plus a semi-colon -- for the list of <em>HQL</em> commands.
 Data Definition, SHELL, INSERTS, DELETES, and UPDATE commands are disabled in this interface
 </p>
 
 <%
  if (query.length() > 0) {
 %>
 <hr/>
 <%
    HQLParser parser = new HQLParser(query, out, new HtmlTableFormatter(out));
    Command cmd = parser.terminatedCommand();
    if (cmd.getCommandType() != Command.CommandType.SELECT) {
 %>
  <p><%= cmd.getCommandType() %>-type commands are disabled in this interface.</p>
 <%
    } else { 
      ReturnMsg rm = cmd.execute(new HBaseConfiguration());
      String summary = rm == null? "": rm.toString();
 %>
  <p><%= summary %></p>
 <% } 
  }
 %>
</body>
</html>
