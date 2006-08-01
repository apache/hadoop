<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.net.*"
  import="org.apache.hadoop.dfs.*"
  import="org.apache.hadoop.io.*"
  import="org.apache.hadoop.conf.*"
  import="java.text.DateFormat"
%>
<%!
  static JspHelper jspHelper = new JspHelper();
  
  public void generateDirectoryStructure(JspWriter out, HttpServletRequest req) 
    throws IOException {
    String dir = req.getParameter("dir");
    if (dir == null || dir.length() == 0) {
      out.print("Invalid input");
      return;
    }

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);
    
    DFSClient dfs = new DFSClient(jspHelper.nameNodeAddr, jspHelper.conf);
    DFSFileInfo[] files = dfs.listPaths(new UTF8(dir));
    //generate a table and dump the info
    String [] headings = new String[5];
    headings[0] = "Name"; headings[1] = "Type"; headings[2] = "Size";
    headings[3] = "Replication"; headings[4] = "BlockSize";
    out.print("<h3>Contents of directory " + dir + "</h3><hr>");

    File f = new File(dir);
    String parent;
    if ((parent = f.getParent()) != null)
      out.print("<a href=\"" + req.getRequestURL() + "?dir=" + parent +
                "&namenodeInfoPort=" + namenodeInfoPort +
                "\">Go to parent directory</a><br>");

    if (files == null || files.length == 0) {
      out.print("Empty directory");
      dfs.close();
      return;
    }

    jspHelper.addTableHeader(out);
    jspHelper.addTableRow(out, headings);
    String cols [] = new String[5];
    for (int i = 0; i < files.length; i++) {
      //Get the location of the first block of the file
      if (files[i].getPath().endsWith(".crc")) continue;
      if (!files[i].isDir()) {
        LocatedBlock[] blocks = dfs.namenode.open(files[i].getPath());
        DatanodeInfo [] locations = blocks[0].getLocations();
        if (locations.length == 0) {
          cols[0] = files[i].getPath();
          cols[1] = "file";
          cols[2] = Long.toString(files[i].getLen());
          cols[3] = Short.toString(files[i].getReplication());
          cols[4] = Long.toString(files[i].getBlockSize());
          jspHelper.addTableRow(out, cols);
          continue;
        }
        DatanodeInfo chosenNode = jspHelper.bestNode(blocks[0]);
        String fqdn = InetAddress.getByName(chosenNode.getHost()).getCanonicalHostName();
        String datanodeAddr = chosenNode.getName();
        int datanodePort = Integer.parseInt(
                                  datanodeAddr.substring(
                                        datanodeAddr.indexOf(':') + 1, 
                                  datanodeAddr.length())); 
        String datanodeUrl = "http://"+fqdn+":" +
                             chosenNode.getInfoPort() + 
                             "/browseBlock.jsp?blockId=" +
                             blocks[0].getBlock().getBlockId() +
                             "&blockSize=" + files[i].getBlockSize() +
               "&filename=" + URLEncoder.encode(files[i].getPath(), "UTF-8") + 
                             "&datanodePort=" + datanodePort + 
                             "&namenodeInfoPort=" + namenodeInfoPort;
        cols[0] = "<a href=\""+datanodeUrl+"\">"+files[i].getPath()+"</a>";
        cols[1] = "file";
        cols[2] = Long.toString(files[i].getLen());
        cols[3] = Short.toString(files[i].getReplication());
        cols[4] = Long.toString(files[i].getBlockSize());
        jspHelper.addTableRow(out, cols);
      }
      else {
        String datanodeUrl = req.getRequestURL()+"?dir="+
            URLEncoder.encode(files[i].getPath(), "UTF-8") + 
            "&namenodeInfoPort=" + namenodeInfoPort;
        cols[0] = "<a href=\""+datanodeUrl+"\">"+files[i].getPath()+"</a>";
        cols[1] = "dir";
        cols[2] = "0";
        cols[3] = Short.toString(files[i].getReplication());
        cols[4] = Long.toString(files[i].getBlockSize());
        jspHelper.addTableRow(out, cols);
      }
    }
    jspHelper.addTableFooter(out);
    String namenodeHost = jspHelper.nameNodeAddr.getHostName();
    out.print("<br><a href=\"http://" + 
              InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":" +
              namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
  }

%>

<html>

<title>Hadoop DFS Directory Browsing</title>

<body>

<% 
   generateDirectoryStructure(out,request);
%>
<hr>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
