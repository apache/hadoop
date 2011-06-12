/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.datanode.DatanodeJspHelper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

import org.znerd.xmlenc.*;

class NamenodeJspHelper {
  static String getSafeModeText(FSNamesystem fsn) {
    if (!fsn.isInSafeMode())
      return "";
    return "Safe mode is ON. <em>" + fsn.getSafeModeTip() + "</em><br>";
  }

  static String getInodeLimitText(FSNamesystem fsn) {
    long inodes = fsn.dir.totalInodes();
    long blocks = fsn.getBlocksTotal();
    long maxobjects = fsn.getMaxObjects();

    MemoryMXBean mem = ManagementFactory.getMemoryMXBean();
    MemoryUsage heap = mem.getHeapMemoryUsage();
    long totalMemory = heap.getUsed();
    long maxMemory = heap.getMax();
    long commitedMemory = heap.getCommitted();
    
    MemoryUsage nonHeap = mem.getNonHeapMemoryUsage();
    long totalNonHeap = nonHeap.getUsed();
    long maxNonHeap = nonHeap.getMax();
    long commitedNonHeap = nonHeap.getCommitted();

    long used = (totalMemory * 100) / commitedMemory;
    long usedNonHeap = (totalNonHeap * 100) / commitedNonHeap;

    String str = inodes + " files and directories, " + blocks + " blocks = "
        + (inodes + blocks) + " total";
    if (maxobjects != 0) {
      long pct = ((inodes + blocks) * 100) / maxobjects;
      str += " / " + maxobjects + " (" + pct + "%)";
    }
    str += ".<br>";
    str += "Heap Memory used " + StringUtils.byteDesc(totalMemory) + " is "
        + " " + used + "% of Commited Heap Memory " 
        + StringUtils.byteDesc(commitedMemory)
        + ". Max Heap Memory is " + StringUtils.byteDesc(maxMemory) +
        ". <br>";
    str += "Non Heap Memory used " + StringUtils.byteDesc(totalNonHeap) + " is"
        + " " + usedNonHeap + "% of " + " Commited Non Heap Memory "
        + StringUtils.byteDesc(commitedNonHeap) + ". Max Non Heap Memory is "
        + StringUtils.byteDesc(maxNonHeap) + ".<br>";
    return str;
  }

  static String getUpgradeStatusText(FSNamesystem fsn) {
    String statusText = "";
    try {
      UpgradeStatusReport status = fsn
          .distributedUpgradeProgress(UpgradeAction.GET_STATUS);
      statusText = (status == null ? "There are no upgrades in progress."
          : status.getStatusText(false));
    } catch (IOException e) {
      statusText = "Upgrade status unknown.";
    }
    return statusText;
  }

  /** Return a table containing version information. */
  static String getVersionTable(FSNamesystem fsn) {
    return "<div id='dfstable'><table>"
        + "\n  <tr><td id='col1'>Started:</td><td>" + fsn.getStartTime()
        + "</td></tr>\n" + "\n  <tr><td id='col1'>Version:</td><td>"
        + VersionInfo.getVersion() + ", " + VersionInfo.getRevision()
        + "\n  <tr><td id='col1'>Compiled:</td><td>" + VersionInfo.getDate()
        + " by " + VersionInfo.getUser() + " from " + VersionInfo.getBranch()
        + "\n  <tr><td id='col1'>Upgrades:</td><td>"
        + getUpgradeStatusText(fsn) + "\n</table></div>";
  }

  static String getWarningText(FSNamesystem fsn) {
    // Ideally this should be displayed in RED
    long missingBlocks = fsn.getMissingBlocksCount();
    if (missingBlocks > 0) {
      return "<br> WARNING :" + " There are about " + missingBlocks
          + " missing blocks. Please check the log or run fsck. <br><br>";
    }
    return "";
  }

  static class HealthJsp {
    private int rowNum = 0;
    private int colNum = 0;
    private String sorterField = null;
    private String sorterOrder = null;

    private String rowTxt() {
      colNum = 0;
      return "<tr class=\"" + (((rowNum++) % 2 == 0) ? "rowNormal" : "rowAlt")
          + "\"> ";
    }

    private String colTxt() {
      return "<td id=\"col" + ++colNum + "\"> ";
    }

    private void counterReset() {
      colNum = 0;
      rowNum = 0;
    }

    void generateConfReport(JspWriter out, NameNode nn,
        HttpServletRequest request) throws IOException {
      FSNamesystem fsn = nn.getNamesystem();
      FSImage fsImage = fsn.getFSImage();
      List<Storage.StorageDirectory> removedStorageDirs = fsImage
          .getRemovedStorageDirs();

      // FS Image storage configuration
      out.print("<h3> " + nn.getRole() + " Storage: </h3>");
      out.print("<div id=\"dfstable\"> <table border=1 cellpadding=10 cellspacing=0 title=\"NameNode Storage\">\n"
              + "<thead><tr><td><b>Storage Directory</b></td><td><b>Type</b></td><td><b>State</b></td></tr></thead>");

      StorageDirectory st = null;
      for (Iterator<StorageDirectory> it = fsImage.dirIterator(); it.hasNext();) {
        st = it.next();
        String dir = "" + st.getRoot();
        String type = "" + st.getStorageDirType();
        out.print("<tr><td>" + dir + "</td><td>" + type
            + "</td><td>Active</td></tr>");
      }

      long storageDirsSize = removedStorageDirs.size();
      for (int i = 0; i < storageDirsSize; i++) {
        st = removedStorageDirs.get(i);
        String dir = "" + st.getRoot();
        String type = "" + st.getStorageDirType();
        out.print("<tr><td>" + dir + "</td><td>" + type
            + "</td><td><font color=red>Failed</font></td></tr>");
      }

      out.print("</table></div><br>\n");
    }

    void generateHealthReport(JspWriter out, NameNode nn,
        HttpServletRequest request) throws IOException {
      FSNamesystem fsn = nn.getNamesystem();
      ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
      fsn.DFSNodesStatus(live, dead);

      ArrayList<DatanodeDescriptor> decommissioning = fsn
          .getDecommissioningNodes();

      sorterField = request.getParameter("sorter/field");
      sorterOrder = request.getParameter("sorter/order");
      if (sorterField == null)
        sorterField = "name";
      if (sorterOrder == null)
        sorterOrder = "ASC";

      // Find out common suffix. Should this be before or after the sort?
      String port_suffix = null;
      if (live.size() > 0) {
        String name = live.get(0).getName();
        int idx = name.indexOf(':');
        if (idx > 0) {
          port_suffix = name.substring(idx);
        }

        for (int i = 1; port_suffix != null && i < live.size(); i++) {
          if (live.get(i).getName().endsWith(port_suffix) == false) {
            port_suffix = null;
            break;
          }
        }
      }

      counterReset();
      long[] fsnStats = fsn.getStats();
      long total = fsnStats[0];
      long remaining = fsnStats[2];
      long used = fsnStats[1];
      long nonDFS = total - remaining - used;
      nonDFS = nonDFS < 0 ? 0 : nonDFS;
      float percentUsed = total <= 0 ? 0f : ((float) used * 100.0f)
          / (float) total;
      float percentRemaining = total <= 0 ? 100f : ((float) remaining * 100.0f)
          / (float) total;

      out.print("<div id=\"dfstable\"> <table>\n" + rowTxt() + colTxt()
          + "Configured Capacity" + colTxt() + ":" + colTxt()
          + StringUtils.byteDesc(total) + rowTxt() + colTxt() + "DFS Used"
          + colTxt() + ":" + colTxt() + StringUtils.byteDesc(used) + rowTxt()
          + colTxt() + "Non DFS Used" + colTxt() + ":" + colTxt()
          + StringUtils.byteDesc(nonDFS) + rowTxt() + colTxt()
          + "DFS Remaining" + colTxt() + ":" + colTxt()
          + StringUtils.byteDesc(remaining) + rowTxt() + colTxt() + "DFS Used%"
          + colTxt() + ":" + colTxt()
          + StringUtils.limitDecimalTo2(percentUsed) + " %" + rowTxt()
          + colTxt() + "DFS Remaining%" + colTxt() + ":" + colTxt()
          + StringUtils.limitDecimalTo2(percentRemaining) + " %" + rowTxt()
          + colTxt()
          + "<a href=\"dfsnodelist.jsp?whatNodes=LIVE\">Live Nodes</a> "
          + colTxt() + ":" + colTxt() + live.size() + rowTxt() + colTxt()
          + "<a href=\"dfsnodelist.jsp?whatNodes=DEAD\">Dead Nodes</a> "
          + colTxt() + ":" + colTxt() + dead.size() + rowTxt() + colTxt()
          + "<a href=\"dfsnodelist.jsp?whatNodes=DECOMMISSIONING\">"
          + "Decommissioning Nodes</a> "
          + colTxt() + ":" + colTxt() + decommissioning.size() 
          + rowTxt() + colTxt()
          + "Number of Under-Replicated Blocks" + colTxt() + ":" + colTxt()
          + fsn.getUnderReplicatedBlocks()
          + "</table></div><br>\n");

      if (live.isEmpty() && dead.isEmpty()) {
        out.print("There are no datanodes in the cluster");
      }
    }
  }

  static String getDelegationToken(final NameNode nn, final String user
                                   ) throws IOException, InterruptedException {
    if (user == null) {
      return null;
    }
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    Token<DelegationTokenIdentifier> token =
      ugi.doAs(
               new PrivilegedExceptionAction<Token<DelegationTokenIdentifier>>() {
                 public Token<DelegationTokenIdentifier> run() throws IOException {
                   return nn.getDelegationToken(new Text(user));
                 }
               });
    return token.encodeToUrlString();
  }

  static void redirectToRandomDataNode(final NameNode nn, 
                                       HttpServletRequest request,
                                       HttpServletResponse resp,
                                       Configuration conf
                                       ) throws IOException,
                                                InterruptedException {
    final DatanodeID datanode = nn.getNamesystem().getRandomDatanode();
    final String user = request.getRemoteUser();
    String tokenString = getDelegationToken(nn, user);
    // if the user is defined, get a delegation token and stringify it
    final String redirectLocation;
    final String nodeToRedirect;
    int redirectPort;
    if (datanode != null) {
      nodeToRedirect = datanode.getHost();
      redirectPort = datanode.getInfoPort();
    } else {
      nodeToRedirect = nn.getHttpAddress().getHostName();
      redirectPort = nn.getHttpAddress().getPort();
    }
    String fqdn = InetAddress.getByName(nodeToRedirect).getCanonicalHostName();
    redirectLocation = "http://" + fqdn + ":" + redirectPort
        + "/browseDirectory.jsp?namenodeInfoPort="
        + nn.getHttpAddress().getPort() + "&dir=/"
        + (tokenString == null ? "" :
           JspHelper.SET_DELEGATION + tokenString);
    resp.sendRedirect(redirectLocation);
  }

  static class NodeListJsp {
    private int rowNum = 0;

    private long diskBytes = 1024 * 1024 * 1024;
    private String diskByteStr = "GB";

    private String sorterField = null;
    private String sorterOrder = null;

    private String whatNodes = "LIVE";

    private String rowTxt() {
      return "<tr class=\"" + (((rowNum++) % 2 == 0) ? "rowNormal" : "rowAlt")
          + "\"> ";
    }

    private void counterReset() {
      rowNum = 0;
    }

    private String nodeHeaderStr(String name) {
      String ret = "class=header";
      String order = "ASC";
      if (name.equals(sorterField)) {
        ret += sorterOrder;
        if (sorterOrder.equals("ASC"))
          order = "DSC";
      }
      ret += " onClick=\"window.document.location="
          + "'/dfsnodelist.jsp?whatNodes=" + whatNodes + "&sorter/field="
          + name + "&sorter/order=" + order
          + "'\" title=\"sort on this column\"";

      return ret;
    }

    void generateDecommissioningNodeData(JspWriter out, DatanodeDescriptor d,
        String suffix, boolean alive, int nnHttpPort) throws IOException {
      String url = "http://" + d.getHostName() + ":" + d.getInfoPort()
          + "/browseDirectory.jsp?namenodeInfoPort=" + nnHttpPort + "&dir="
          + URLEncoder.encode("/", "UTF-8");

      String name = d.getHostName() + ":" + d.getPort();
      if (!name.matches("\\d+\\.\\d+.\\d+\\.\\d+.*"))
        name = name.replaceAll("\\.[^.:]*", "");
      int idx = (suffix != null && name.endsWith(suffix)) ? name
          .indexOf(suffix) : -1;

      out.print(rowTxt() + "<td class=\"name\"><a title=\"" + d.getHost() + ":"
          + d.getPort() + "\" href=\"" + url + "\">"
          + ((idx > 0) ? name.substring(0, idx) : name) + "</a>"
          + ((alive) ? "" : "\n"));
      if (!alive) {
        return;
      }

      long decommRequestTime = d.decommissioningStatus.getStartTime();
      long timestamp = d.getLastUpdate();
      long currentTime = System.currentTimeMillis();
      long hoursSinceDecommStarted = (currentTime - decommRequestTime)/3600000;
      long remainderMinutes = ((currentTime - decommRequestTime)/60000) % 60;
      out.print("<td class=\"lastcontact\"> "
          + ((currentTime - timestamp) / 1000)
          + "<td class=\"underreplicatedblocks\">"
          + d.decommissioningStatus.getUnderReplicatedBlocks()
          + "<td class=\"blockswithonlydecommissioningreplicas\">"
          + d.decommissioningStatus.getDecommissionOnlyReplicas() 
          + "<td class=\"underrepblocksinfilesunderconstruction\">"
          + d.decommissioningStatus.getUnderReplicatedInOpenFiles()
          + "<td class=\"timesincedecommissionrequest\">"
          + hoursSinceDecommStarted + " hrs " + remainderMinutes + " mins"
          + "\n");
    }
    
    void generateNodeData(JspWriter out, DatanodeDescriptor d,
        String suffix, boolean alive, int nnHttpPort) throws IOException {
      /*
       * Say the datanode is dn1.hadoop.apache.org with ip 192.168.0.5 we use:
       * 1) d.getHostName():d.getPort() to display. Domain and port are stripped
       *    if they are common across the nodes. i.e. "dn1"
       * 2) d.getHost():d.Port() for "title". i.e. "192.168.0.5:50010"
       * 3) d.getHostName():d.getInfoPort() for url.
       *    i.e. "http://dn1.hadoop.apache.org:50075/..."
       * Note that "d.getHost():d.getPort()" is what DFS clients use to
       * interact with datanodes.
       */

      // from nn_browsedfscontent.jsp:
      String url = "http://" + d.getHostName() + ":" + d.getInfoPort()
          + "/browseDirectory.jsp?namenodeInfoPort=" + nnHttpPort + "&dir="
          + URLEncoder.encode("/", "UTF-8");

      String name = d.getHostName() + ":" + d.getPort();
      if (!name.matches("\\d+\\.\\d+.\\d+\\.\\d+.*"))
        name = name.replaceAll("\\.[^.:]*", "");
      int idx = (suffix != null && name.endsWith(suffix)) ? name
          .indexOf(suffix) : -1;

      out.print(rowTxt() + "<td class=\"name\"><a title=\"" + d.getHost() + ":"
          + d.getPort() + "\" href=\"" + url + "\">"
          + ((idx > 0) ? name.substring(0, idx) : name) + "</a>"
          + ((alive) ? "" : "\n"));
      if (!alive)
        return;

      long c = d.getCapacity();
      long u = d.getDfsUsed();
      long nu = d.getNonDfsUsed();
      long r = d.getRemaining();
      String percentUsed = StringUtils.limitDecimalTo2(d.getDfsUsedPercent());
      String percentRemaining = StringUtils.limitDecimalTo2(d
          .getRemainingPercent());

      String adminState = (d.isDecommissioned() ? "Decommissioned" : (d
          .isDecommissionInProgress() ? "Decommission In Progress"
          : "In Service"));

      long timestamp = d.getLastUpdate();
      long currentTime = System.currentTimeMillis();
      out.print("<td class=\"lastcontact\"> "
          + ((currentTime - timestamp) / 1000)
          + "<td class=\"adminstate\">"
          + adminState
          + "<td align=\"right\" class=\"capacity\">"
          + StringUtils.limitDecimalTo2(c * 1.0 / diskBytes)
          + "<td align=\"right\" class=\"used\">"
          + StringUtils.limitDecimalTo2(u * 1.0 / diskBytes)
          + "<td align=\"right\" class=\"nondfsused\">"
          + StringUtils.limitDecimalTo2(nu * 1.0 / diskBytes)
          + "<td align=\"right\" class=\"remaining\">"
          + StringUtils.limitDecimalTo2(r * 1.0 / diskBytes)
          + "<td align=\"right\" class=\"pcused\">"
          + percentUsed
          + "<td class=\"pcused\">"
          + ServletUtil.percentageGraph((int) Double.parseDouble(percentUsed),
              100) + "<td align=\"right\" class=\"pcremaining`\">"
          + percentRemaining + "<td title=" + "\"blocks scheduled : "
          + d.getBlocksScheduled() + "\" class=\"blocks\">" + d.numBlocks()
          + "\n");
    }

    void generateNodesList(JspWriter out, NameNode nn,
        HttpServletRequest request) throws IOException {
      ArrayList<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      ArrayList<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
      nn.getNamesystem().DFSNodesStatus(live, dead);

      whatNodes = request.getParameter("whatNodes"); // show only live or only
                                                     // dead nodes
      sorterField = request.getParameter("sorter/field");
      sorterOrder = request.getParameter("sorter/order");
      if (sorterField == null)
        sorterField = "name";
      if (sorterOrder == null)
        sorterOrder = "ASC";

      JspHelper.sortNodeList(live, sorterField, sorterOrder);
      JspHelper.sortNodeList(dead, "name", "ASC");

      // Find out common suffix. Should this be before or after the sort?
      String port_suffix = null;
      if (live.size() > 0) {
        String name = live.get(0).getName();
        int idx = name.indexOf(':');
        if (idx > 0) {
          port_suffix = name.substring(idx);
        }

        for (int i = 1; port_suffix != null && i < live.size(); i++) {
          if (live.get(i).getName().endsWith(port_suffix) == false) {
            port_suffix = null;
            break;
          }
        }
      }

      counterReset();

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }

      if (live.isEmpty() && dead.isEmpty()) {
        out.print("There are no datanodes in the cluster");
      } else {

        int nnHttpPort = nn.getHttpAddress().getPort();
        out.print("<div id=\"dfsnodetable\"> ");
        if (whatNodes.equals("LIVE")) {

          out.print("<a name=\"LiveNodes\" id=\"title\">" + "Live Datanodes : "
              + live.size() + "</a>"
              + "<br><br>\n<table border=1 cellspacing=0>\n");

          counterReset();

          if (live.size() > 0) {
            if (live.get(0).getCapacity() > 1024 * diskBytes) {
              diskBytes *= 1024;
              diskByteStr = "TB";
            }

            out.print("<tr class=\"headerRow\"> <th " + nodeHeaderStr("name")
                + "> Node <th " + nodeHeaderStr("lastcontact")
                + "> Last <br>Contact <th " + nodeHeaderStr("adminstate")
                + "> Admin State <th " + nodeHeaderStr("capacity")
                + "> Configured <br>Capacity (" + diskByteStr + ") <th "
                + nodeHeaderStr("used") + "> Used <br>(" + diskByteStr
                + ") <th " + nodeHeaderStr("nondfsused")
                + "> Non DFS <br>Used (" + diskByteStr + ") <th "
                + nodeHeaderStr("remaining") + "> Remaining <br>("
                + diskByteStr + ") <th " + nodeHeaderStr("pcused")
                + "> Used <br>(%) <th " + nodeHeaderStr("pcused")
                + "> Used <br>(%) <th " + nodeHeaderStr("pcremaining")
                + "> Remaining <br>(%) <th " + nodeHeaderStr("blocks")
                + "> Blocks\n");

            JspHelper.sortNodeList(live, sorterField, sorterOrder);
            for (int i = 0; i < live.size(); i++) {
              generateNodeData(out, live.get(i), port_suffix, true, nnHttpPort);
            }
          }
          out.print("</table>\n");
        } else if (whatNodes.equals("DEAD")) {

          out.print("<br> <a name=\"DeadNodes\" id=\"title\"> "
              + " Dead Datanodes : " + dead.size() + "</a><br><br>\n");

          if (dead.size() > 0) {
            out.print("<table border=1 cellspacing=0> <tr id=\"row1\"> "
                + "<td> Node \n");

            JspHelper.sortNodeList(dead, "name", "ASC");
            for (int i = 0; i < dead.size(); i++) {
              generateNodeData(out, dead.get(i), port_suffix, false, nnHttpPort);
            }

            out.print("</table>\n");
          }
        } else if (whatNodes.equals("DECOMMISSIONING")) {
          // Decommissioning Nodes
          ArrayList<DatanodeDescriptor> decommissioning = nn.getNamesystem()
              .getDecommissioningNodes();
          out.print("<br> <a name=\"DecommissioningNodes\" id=\"title\"> "
              + " Decommissioning Datanodes : " + decommissioning.size()
              + "</a><br><br>\n");
          if (decommissioning.size() > 0) {
            out.print("<table border=1 cellspacing=0> <tr class=\"headRow\"> "
                + "<th " + nodeHeaderStr("name") 
                + "> Node <th " + nodeHeaderStr("lastcontact")
                + "> Last <br>Contact <th "
                + nodeHeaderStr("underreplicatedblocks")
                + "> Under Replicated Blocks <th "
                + nodeHeaderStr("blockswithonlydecommissioningreplicas")
                + "> Blocks With No <br> Live Replicas <th "
                + nodeHeaderStr("underrepblocksinfilesunderconstruction")
                + "> Under Replicated Blocks <br> In Files Under Construction" 
                + " <th " + nodeHeaderStr("timesincedecommissionrequest")
                + "> Time Since Decommissioning Started"
                );

            JspHelper.sortNodeList(decommissioning, "name", "ASC");
            for (int i = 0; i < decommissioning.size(); i++) {
              generateDecommissioningNodeData(out, decommissioning.get(i),
                  port_suffix, true, nnHttpPort);
            }
            out.print("</table>\n");
          }
        }
        out.print("</div>");
      }
    }
  }
  
  // utility class used in block_info_xml.jsp
  static class XMLBlockInfo {
    final Block block;
    final INodeFile inode;
    final FSNamesystem fsn;
    
    public XMLBlockInfo(FSNamesystem fsn, Long blockId) {
      this.fsn = fsn;
      if (blockId == null) {
        this.block = null;
        this.inode = null;
      } else {
        this.block = new Block(blockId);
        this.inode = fsn.blockManager.getINode(block);
      }
    }

    private String getLocalParentDir(INode inode) {
      StringBuilder pathBuf = new StringBuilder();
      INode node = inode;
      
      // loop up to directory root, prepending each directory name to buffer
      while ((node = node.getParent()) != null && node.getLocalName() != "") {
        pathBuf.insert(0, '/').insert(0, node.getLocalName());
      }

      return pathBuf.toString();
    }

    public void toXML(XMLOutputter doc) throws IOException {
      doc.startTag("block_info");
      if (block == null) {
        doc.startTag("error");
        doc.pcdata("blockId must be a Long");
        doc.endTag();
      }else{
        doc.startTag("block_id");
        doc.pcdata(""+block.getBlockId());
        doc.endTag();

        doc.startTag("block_name");
        doc.pcdata(block.getBlockName());
        doc.endTag();

        if (inode != null) {
          doc.startTag("file");

          doc.startTag("local_name");
          doc.pcdata(inode.getLocalName());
          doc.endTag();

          doc.startTag("local_directory");
          doc.pcdata(getLocalParentDir(inode));
          doc.endTag();

          doc.startTag("user_name");
          doc.pcdata(inode.getUserName());
          doc.endTag();

          doc.startTag("group_name");
          doc.pcdata(inode.getGroupName());
          doc.endTag();

          doc.startTag("is_directory");
          doc.pcdata(""+inode.isDirectory());
          doc.endTag();

          doc.startTag("access_time");
          doc.pcdata(""+inode.getAccessTime());
          doc.endTag();

          doc.startTag("is_under_construction");
          doc.pcdata(""+inode.isUnderConstruction());
          doc.endTag();

          doc.startTag("ds_quota");
          doc.pcdata(""+inode.getDsQuota());
          doc.endTag();

          doc.startTag("permission_status");
          doc.pcdata(inode.getPermissionStatus().toString());
          doc.endTag();

          doc.startTag("replication");
          doc.pcdata(""+inode.getReplication());
          doc.endTag();

          doc.startTag("disk_space_consumed");
          doc.pcdata(""+inode.diskspaceConsumed());
          doc.endTag();

          doc.startTag("preferred_block_size");
          doc.pcdata(""+inode.getPreferredBlockSize());
          doc.endTag();

          doc.endTag(); // </file>
        } 

        doc.startTag("replicas");
       
        if (fsn.blockManager.blocksMap.contains(block)) {
          Iterator<DatanodeDescriptor> it =
            fsn.blockManager.blocksMap.nodeIterator(block);

          while (it.hasNext()) {
            doc.startTag("replica");

            DatanodeDescriptor dd = it.next();

            doc.startTag("host_name");
            doc.pcdata(dd.getHostName());
            doc.endTag();

            boolean isCorrupt = fsn.getCorruptReplicaBlockIds(0,
                                  block.getBlockId()) != null;
            
            doc.startTag("is_corrupt");
            doc.pcdata(""+isCorrupt);
            doc.endTag();
            
            doc.endTag(); // </replica>
          }

        } 
        doc.endTag(); // </replicas>
                
      }
      
      doc.endTag(); // </block_info>
      
    }
  }
  
  // utility class used in corrupt_replicas_xml.jsp
  static class XMLCorruptBlockInfo {
    final FSNamesystem fsn;
    final Configuration conf;
    final Long startingBlockId;
    final int numCorruptBlocks;
    
    public XMLCorruptBlockInfo(FSNamesystem fsn, Configuration conf,
                               int numCorruptBlocks, Long startingBlockId) {
      this.fsn = fsn;
      this.conf = conf;
      this.numCorruptBlocks = numCorruptBlocks;
      this.startingBlockId = startingBlockId;
    }


    public void toXML(XMLOutputter doc) throws IOException {
      
      doc.startTag("corrupt_block_info");
      
      if (numCorruptBlocks < 0 || numCorruptBlocks > 100) {
        doc.startTag("error");
        doc.pcdata("numCorruptBlocks must be >= 0 and <= 100");
        doc.endTag();
      }
      
      doc.startTag("dfs_replication");
      doc.pcdata(""+conf.getInt("dfs.replication", 3));
      doc.endTag();
      
      doc.startTag("num_missing_blocks");
      doc.pcdata(""+fsn.getMissingBlocksCount());
      doc.endTag();
      
      doc.startTag("num_corrupt_replica_blocks");
      doc.pcdata(""+fsn.getCorruptReplicaBlocks());
      doc.endTag();
     
      doc.startTag("corrupt_replica_block_ids");
      long[] corruptBlockIds
        = fsn.getCorruptReplicaBlockIds(numCorruptBlocks,
                                        startingBlockId);
      if (corruptBlockIds != null) {
        for (Long blockId: corruptBlockIds) {
          doc.startTag("block_id");
          doc.pcdata(""+blockId);
          doc.endTag();
        }
      }
      
      doc.endTag(); // </corrupt_replica_block_ids>

      doc.endTag(); // </corrupt_block_info>
      
      doc.getWriter().flush();
    }
  }    
}
