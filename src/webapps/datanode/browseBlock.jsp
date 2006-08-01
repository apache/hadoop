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

  public void generateFileDetails(JspWriter out, HttpServletRequest req) 
    throws IOException {

    int chunkSizeToView = 0;
    long startOffset = 0;
    int datanodePort;

    String blockIdStr = null;
    long currBlockId = 0;
    blockIdStr = req.getParameter("blockId");
    if (blockIdStr == null) {
      out.print("Invalid input (blockId absent)");
      return;
    }
    currBlockId = Long.parseLong(blockIdStr);

    String datanodePortStr = req.getParameter("datanodePort");
    if (datanodePortStr == null) {
      out.print("Invalid input (datanodePort absent)");
      return;
    }
    datanodePort = Integer.parseInt(datanodePortStr);

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    String chunkSizeToViewStr = req.getParameter("chunkSizeToView");
    if (chunkSizeToViewStr != null && Integer.parseInt(chunkSizeToViewStr) > 0)
      chunkSizeToView = Integer.parseInt(chunkSizeToViewStr);
    else chunkSizeToView = jspHelper.defaultChunkSizeToView;

    String startOffsetStr = req.getParameter("startOffset");
    if (startOffsetStr == null || Long.parseLong(startOffsetStr) < 0)
      startOffset = 0;
    else startOffset = Long.parseLong(startOffsetStr);
    
    String filename = req.getParameter("filename");
    if (filename == null || filename.length() == 0) {
      out.print("Invalid input");
      return;
    }

    String blockSizeStr = req.getParameter("blockSize"); 
    long blockSize = 0;
    if (blockSizeStr == null && blockSizeStr.length() == 0) {
      out.print("Invalid input");
      return;
    } 
    blockSize = Long.parseLong(blockSizeStr);

    DFSClient dfs = new DFSClient(jspHelper.nameNodeAddr, jspHelper.conf);
    LocatedBlock[] blocks = dfs.namenode.open(filename);
    //Add the various links for looking at the file contents
    //URL for downloading the full file
    String downloadUrl = "http://" + req.getServerName() + ":" +
                         + req.getServerPort() + "/streamFile?" + "filename=" +
                         URLEncoder.encode(filename, "UTF-8");
    out.print("<a name=\"viewOptions\"></a>");
    out.print("<a href=\"" + downloadUrl + "\">Download this file</a><br>");
    
    DatanodeInfo chosenNode;
    //URL for TAIL 
    LocatedBlock lastBlk = blocks[blocks.length - 1];
    long blockId = lastBlk.getBlock().getBlockId();
    try {
      chosenNode = jspHelper.bestNode(lastBlk);
    } catch (IOException e) {
      out.print(e.toString());
      dfs.close();
      return;
    }
    String fqdn = 
           InetAddress.getByName(chosenNode.getHost()).getCanonicalHostName();
    String tailUrl = "http://" + fqdn + ":" +
                     chosenNode.getInfoPort() + 
                 "/tail.jsp?filename=" + URLEncoder.encode(filename, "UTF-8") +
                 "&chunkSizeToView=" + chunkSizeToView +
                 "&referrer=" + 
          URLEncoder.encode(req.getRequestURL() + "?" + req.getQueryString(),
                            "UTF-8");
    out.print("<a href=\"" + tailUrl + "\">TAIL this file</a><br>");

    out.print("<form action=\"/browseBlock.jsp\" method=GET>");
    out.print("<b>Chunk Size to view (in bytes, upto file's DFS blocksize): </b>");
    out.print("<input type=\"hidden\" name=\"blockId\" value=\"" + currBlockId +
              "\">");
    out.print("<input type=\"hidden\" name=\"blockSize\" value=\"" + 
              blockSize + "\">");
    out.print("<input type=\"hidden\" name=\"startOffset\" value=\"" + 
              startOffset + "\">");
    out.print("<input type=\"hidden\" name=\"filename\" value=\"" + filename +
              "\">");
    out.print("<input type=\"hidden\" name=\"datanodePort\" value=\"" + 
              datanodePort+ "\">");
    out.print("<input type=\"hidden\" name=\"namenodeInfoPort\" value=\"" +
              namenodeInfoPort + "\">");
    out.print("<input type=\"text\" name=\"chunkSizeToView\" value=" +
              chunkSizeToView + " size=10 maxlength=10>");
    out.print("&nbsp;&nbsp;<input type=\"submit\" name=\"submit\" value=\"Refresh\">");
    out.print("</form>");
    out.print("<hr>"); 
    out.print("<a name=\"blockDetails\"></a>");
    out.print("<B>Total number of blocks: "+blocks.length+"</B><br>");
    //generate a table and dump the info
    for (int i = 0; i < blocks.length; i++) {
      blockId = blocks[i].getBlock().getBlockId();
      blockSize = blocks[i].getBlock().getNumBytes();
      String blk = "blk_" + Long.toString(blockId);
      DatanodeInfo[] locs = blocks[i].getLocations();
      int r = jspHelper.rand.nextInt(locs.length);
      String datanodeAddr = locs[r].getName();
      datanodePort = Integer.parseInt(datanodeAddr.substring(
                                        datanodeAddr.indexOf(':') + 1, 
                                    datanodeAddr.length())); 
      fqdn = InetAddress.getByName(locs[r].getHost()).getCanonicalHostName();
      String blockUrl = "http://"+ fqdn + ":" +
                        locs[r].getInfoPort() +
                        "/browseBlock.jsp?blockId=" + Long.toString(blockId) +
                        "&blockSize=" + blockSize +
               "&filename=" + URLEncoder.encode(filename, "UTF-8")+ 
                        "&datanodePort=" + datanodePort + 
                        "&namenodeInfoPort=" + namenodeInfoPort +
                        "&chunkSizeToView=" + chunkSizeToView;
      out.print("<a href=\"" + blockUrl + "\">" + "blk_" + blockId + 
                        "</a>");
      if (i < blocks.length - 1)
        out.print("&nbsp;&nbsp;&nbsp;&nbsp;");
      if (i % 3 == 0) out.print("<br>");
    }
    out.print("<hr>");
    String namenodeHost = jspHelper.nameNodeAddr.getHostName();
    out.print("<br><a href=\"http://" + 
              InetAddress.getByName(namenodeHost).getCanonicalHostName() + ":" +
              namenodeInfoPort + "/dfshealth.jsp\">Go back to DFS home</a>");
    dfs.close();
  }

  public void generateFileChunks(JspWriter out, HttpServletRequest req) 
    throws IOException {
    long startOffset = 0;
    int datanodePort = 0; 
    int chunkSizeToView = 0;

    String namenodeInfoPortStr = req.getParameter("namenodeInfoPort");
    int namenodeInfoPort = -1;
    if (namenodeInfoPortStr != null)
      namenodeInfoPort = Integer.parseInt(namenodeInfoPortStr);

    String filename = req.getParameter("filename");
    if (filename == null) {
      out.print("Invalid input (filename absent)");
      return;
    }
    
    String blockIdStr = null;
    long blockId = 0;
    blockIdStr = req.getParameter("blockId");
    if (blockIdStr == null) {
      out.print("Invalid input (blockId absent)");
      return;
    }
    blockId = Long.parseLong(blockIdStr);

    String blockSizeStr;
    long blockSize = 0;
    blockSizeStr = req.getParameter("blockSize"); 
    if (blockSizeStr == null) {
      out.print("Invalid input (blockSize absent)");
      return;
    }
    blockSize = Long.parseLong(blockSizeStr);
    
    String chunkSizeToViewStr = req.getParameter("chunkSizeToView");
    if (chunkSizeToViewStr != null && Integer.parseInt(chunkSizeToViewStr) > 0)
      chunkSizeToView = Integer.parseInt(chunkSizeToViewStr);
    else chunkSizeToView = jspHelper.defaultChunkSizeToView;

    String startOffsetStr = req.getParameter("startOffset");
    if (startOffsetStr == null || Long.parseLong(startOffsetStr) < 0)
      startOffset = 0;
    else startOffset = Long.parseLong(startOffsetStr);

    String datanodePortStr = req.getParameter("datanodePort");
    if (datanodePortStr == null) {
      out.print("Invalid input (datanodePort absent)");
      return;
    }
    datanodePort = Integer.parseInt(datanodePortStr);

    out.print("<h2>File: " + filename + "</h2>");
    out.print("<a href=\"http://" + req.getServerName() + ":" + 
              req.getServerPort() + 
              "/browseDirectory.jsp?dir=" + 
              URLEncoder.encode(new File(filename).getParent(), "UTF-8") +
              "&namenodeInfoPort=" + namenodeInfoPort + 
              "\"><i>Go back to dir listing</i></a><br>");
    out.print("<a href=\"#viewOptions\">Advanced view/download options</a><br>");
    out.print("<hr>");

    //Determine the prev & next blocks
    DFSClient dfs = new DFSClient(jspHelper.nameNodeAddr, jspHelper.conf);
    long nextStartOffset = 0;
    long nextBlockSize = 0;
    String nextBlockIdStr = null;
    String nextHost = req.getServerName();
    int nextPort = req.getServerPort();
    int nextDatanodePort = datanodePort;
    //determine data for the next link
    if (startOffset + chunkSizeToView >= blockSize) {
      //we have to go to the next block from this point onwards
      LocatedBlock[] blocks = dfs.namenode.open(filename);
      for (int i = 0; i < blocks.length; i++) {
        if (blocks[i].getBlock().getBlockId() == blockId) {
          if (i != blocks.length - 1) {
            LocatedBlock nextBlock = blocks[i+1];
            nextBlockIdStr = Long.toString(nextBlock.getBlock().getBlockId());
            nextStartOffset = 0;
            nextBlockSize = nextBlock.getBlock().getNumBytes();
            DatanodeInfo d = jspHelper.bestNode(nextBlock);
            String datanodeAddr = d.getName();
            nextDatanodePort = Integer.parseInt(
                                      datanodeAddr.substring(
                                           datanodeAddr.indexOf(':') + 1, 
                                      datanodeAddr.length())); 
            nextHost = InetAddress.getByName(d.getHost()).getCanonicalHostName();
            nextPort = d.getInfoPort(); 
          }
        }
      }
    } 
    else {
      //we are in the same block
      nextBlockIdStr = blockIdStr;
      nextStartOffset = startOffset + chunkSizeToView;
      nextBlockSize = blockSize;
    }
    String nextUrl = null;
    if (nextBlockIdStr != null) {
      nextUrl = "http://" + nextHost + ":" + 
                nextPort + 
                "/browseBlock.jsp?blockId=" + nextBlockIdStr +
                "&blockSize=" + nextBlockSize + "&startOffset=" + 
                nextStartOffset + 
                "&filename=" + URLEncoder.encode(filename, "UTF-8") +
                "&chunkSizeToView=" + chunkSizeToView + 
                "&datanodePort=" + nextDatanodePort +
                "&namenodeInfoPort=" + namenodeInfoPort;
      out.print("<a href=\"" + nextUrl + "\">View Next chunk</a>&nbsp;&nbsp;");        
    }
    //determine data for the prev link
    String prevBlockIdStr = null;
    long prevStartOffset = 0;
    long prevBlockSize = 0;
    String prevHost = req.getServerName();
    int prevPort = req.getServerPort();
    int prevDatanodePort = datanodePort;
    if (startOffset == 0) {
      LocatedBlock [] blocks = dfs.namenode.open(filename);
      for (int i = 0; i < blocks.length; i++) {
        if (blocks[i].getBlock().getBlockId() == blockId) {
          if (i != 0) {
            LocatedBlock prevBlock = blocks[i-1];
            prevBlockIdStr = Long.toString(prevBlock.getBlock().getBlockId());
            prevStartOffset = prevBlock.getBlock().getNumBytes() - chunkSizeToView;
            if (prevStartOffset < 0)
              prevStartOffset = 0;
            prevBlockSize = prevBlock.getBlock().getNumBytes();
            DatanodeInfo d = jspHelper.bestNode(prevBlock);
            String datanodeAddr = d.getName();
            prevDatanodePort = Integer.parseInt(
                                      datanodeAddr.substring(
                                          datanodeAddr.indexOf(':') + 1, 
                                      datanodeAddr.length())); 
            prevHost = InetAddress.getByName(d.getHost()).getCanonicalHostName();
            prevPort = d.getInfoPort();
          }
        }
      }
    }
    else {
      //we are in the same block
      prevBlockIdStr = blockIdStr;
      prevStartOffset = startOffset - chunkSizeToView;
      if (prevStartOffset < 0) prevStartOffset = 0;
      prevBlockSize = blockSize;
    }

    String prevUrl = null;
    if (prevBlockIdStr != null) {
      prevUrl = "http://" + prevHost + ":" + 
                prevPort + 
                "/browseBlock.jsp?blockId=" + prevBlockIdStr + 
                "&blockSize=" + prevBlockSize + "&startOffset=" + 
                prevStartOffset + 
                "&filename=" + URLEncoder.encode(filename, "UTF-8") + 
                "&chunkSizeToView=" + chunkSizeToView +
                "&datanodePort=" + prevDatanodePort +
                "&namenodeInfoPort=" + namenodeInfoPort;
      out.print("<a href=\"" + prevUrl + "\">View Prev chunk</a>&nbsp;&nbsp;");
    }
    out.print("<hr>");
    out.print("<textarea cols=\"100\" rows=\"25\" wrap=\"virtual\" READONLY>");
    try {
    jspHelper.streamBlockInAscii(
            new InetSocketAddress(req.getServerName(), datanodePort), blockId, 
            blockSize, startOffset, chunkSizeToView, out);
    } catch (Exception e){
        out.print(e);
    }
    out.print("</textarea>");
    dfs.close();
  }

%>
<html>

<title>Hadoop DFS File Viewer</title>

<body>
<% 
   generateFileChunks(out,request);
%>
<hr>
<% 
   generateFileDetails(out,request);
%>

<h2>Local logs</h2>
<a href="/logs/">Log</a> directory

<hr>
<a href="http://lucene.apache.org/hadoop">Hadoop</a>, 2006.<br>
</body>
</html>
