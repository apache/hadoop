<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="org.apache.hadoop.dfs.*"
  import="java.text.DateFormat"
  import="java.net.InetAddress"
  import="java.net.URLEncoder"
%>
<%!
  FSNamesystem fsn = FSNamesystem.getFSNamesystem();
  public void redirectToRandomDataNode(HttpServletResponse resp) throws IOException {
    String datanode = fsn.randomDataNode();
    String redirectLocation;
    String nodeToRedirect;
    int redirectPort;
    if (datanode != null) {
      redirectPort = Integer.parseInt(datanode.substring(datanode.indexOf(':') + 1));
      nodeToRedirect = datanode.substring(0, datanode.indexOf(':'));
    }
    else {
      nodeToRedirect = fsn.getDFSNameNodeMachine();
      redirectPort = fsn.getNameNodeInfoPort();
    }
    String fqdn = InetAddress.getByName(nodeToRedirect).getCanonicalHostName();
    redirectLocation = "http://" + fqdn + ":" + redirectPort + 
                       "/browseDirectory.jsp?namenodeInfoPort=" + 
                       fsn.getNameNodeInfoPort() +
                       "&dir=" + URLEncoder.encode("/", "UTF-8");
    resp.sendRedirect(redirectLocation);
  }
%>

<html>

<title></title>

<body>
<% 
   redirectToRandomDataNode(response); 
%>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
