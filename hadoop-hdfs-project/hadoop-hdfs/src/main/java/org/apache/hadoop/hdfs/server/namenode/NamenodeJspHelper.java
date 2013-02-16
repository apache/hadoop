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

import static org.apache.hadoop.hdfs.DFSUtil.percent2String;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.JspWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.VersionInfo;
import org.znerd.xmlenc.XMLOutputter;

import com.google.common.base.Preconditions;

class NamenodeJspHelper {
  static String fraction2String(double value) {
    return StringUtils.format("%.2f", value);
  }

  static String fraction2String(long numerator, long denominator) {
    return fraction2String(numerator/(double)denominator);
  }

  static String getSafeModeText(FSNamesystem fsn) {
    if (!fsn.isInSafeMode())
      return "";
    return "Safe mode is ON. <em>" + fsn.getSafeModeTip() + "</em><br>";
  }

  /**
   * returns security mode of the cluster (namenode)
   * @return "on" if security is on, and "off" otherwise
   */
  static String getSecurityModeText() {
    if(UserGroupInformation.isSecurityEnabled()) {
      return "<div class=\"security\">Security is <em>ON</em></div>";
    } else {
      return "<div class=\"security\">Security is <em>OFF</em></div>";
    }
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

    String str = "<div>" + inodes + " files and directories, " + blocks + " blocks = "
        + (inodes + blocks) + " total filesystem objects";
    if (maxobjects != 0) {
      long pct = ((inodes + blocks) * 100) / maxobjects;
      str += " / " + maxobjects + " (" + pct + "%)";
    }
    str += ".</div>";
    str += "<div>Heap Memory used " + StringUtils.byteDesc(totalMemory) + " is "
        + " " + used + "% of Commited Heap Memory "
        + StringUtils.byteDesc(commitedMemory)
        + ". Max Heap Memory is " + StringUtils.byteDesc(maxMemory) +
        ". </div>";
    str += "<div>Non Heap Memory used " + StringUtils.byteDesc(totalNonHeap) + " is"
        + " " + usedNonHeap + "% of " + " Commited Non Heap Memory "
        + StringUtils.byteDesc(commitedNonHeap) + ". Max Non Heap Memory is "
        + StringUtils.byteDesc(maxNonHeap) + ".</div>";
    return str;
  }

  /** Return a table containing version information. */
  static String getVersionTable(FSNamesystem fsn) {
    return "<div class='dfstable'><table>"
        + "\n  <tr><td class='col1'>Started:</td><td>" + fsn.getStartTime()
        + "</td></tr>\n" + "\n  <tr><td class='col1'>Version:</td><td>"
        + VersionInfo.getVersion() + ", " + VersionInfo.getRevision()
        + "</td></tr>\n" + "\n  <tr><td class='col1'>Compiled:</td><td>" + VersionInfo.getDate()
        + " by " + VersionInfo.getUser() + " from " + VersionInfo.getBranch()
        + "</td></tr>\n  <tr><td class='col1'>Cluster ID:</td><td>" + fsn.getClusterId()
        + "</td></tr>\n  <tr><td class='col1'>Block Pool ID:</td><td>" + fsn.getBlockPoolId()
        + "</td></tr>\n</table></div>";
  }

  /**
   * Generate warning text if there are corrupt files.
   * @return a warning if files are corrupt, otherwise return an empty string.
   */
  static String getCorruptFilesWarning(FSNamesystem fsn) {
    long missingBlocks = fsn.getMissingBlocksCount();
    if (missingBlocks > 0) {
      StringBuilder result = new StringBuilder();

      // Warning class is typically displayed in RED.
      result.append("<div>"); // opening tag of outer <div>.
      result.append("<a class=\"warning\" href=\"/corrupt_files.jsp\" title=\"List corrupt files\">\n");
      result.append("<b>WARNING : There are " + missingBlocks
          + " missing blocks. Please check the logs or run fsck in order to identify the missing blocks.</b>");
      result.append("</a>");

      result.append("<div class=\"small\">See the Hadoop FAQ for common causes and potential solutions.</div>");
      result.append("</div>\n"); // closing tag of outer <div>.

      return result.toString();
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

    private String colTxt(String title) {
      return "<td id=\"col" + ++colNum + "\" title=\"" + title + "\"> ";
    }

    private void counterReset() {
      colNum = 0;
      rowNum = 0;
    }

    void generateConfReport(JspWriter out, NameNode nn,
        HttpServletRequest request) throws IOException {
      FSNamesystem fsn = nn.getNamesystem();
      FSImage fsImage = fsn.getFSImage();
      List<Storage.StorageDirectory> removedStorageDirs 
        = fsImage.getStorage().getRemovedStorageDirs();

      // FS Image storage configuration
      out.print("<h3> " + nn.getRole() + " Storage: </h3>");
      out.print("<div class=\"dfstable\"> <table class=\"storage\" title=\"NameNode Storage\">\n"
              + "<thead><tr><td><b>Storage Directory</b></td><td><b>Type</b></td><td><b>State</b></td></tr></thead>");

      StorageDirectory st = null;
      for (Iterator<StorageDirectory> it
             = fsImage.getStorage().dirIterator(); it.hasNext();) {
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
            + "</td><td><span class=\"failed\">Failed</span></td></tr>");
      }

      out.print("</table></div>\n");
    }
    
    /**
     * Generate an HTML report containing the current status of the HDFS
     * journals.
     */
    void generateJournalReport(JspWriter out, NameNode nn,
        HttpServletRequest request) throws IOException {
      FSEditLog log = nn.getFSImage().getEditLog();
      Preconditions.checkArgument(log != null, "no edit log set in %s", nn);
      
      out.println("<h3> " + nn.getRole() + " Journal Status: </h3>");

      out.println("<b>Current transaction ID:</b> " +
          nn.getFSImage().getLastAppliedOrWrittenTxId() + "<br/>");
      
      
      boolean openForWrite = log.isOpenForWrite();
      
      out.println("<div class=\"dfstable\">");
      out.println("<table class=\"storage\" title=\"NameNode Journals\">\n"
              + "<thead><tr><td><b>Journal Manager</b></td><td><b>State</b></td></tr></thead>");
      for (JournalAndStream jas : log.getJournals()) {
        out.print("<tr>");
        out.print("<td>" + jas.getManager());
        if (jas.isRequired()) {
          out.print(" [required]");
        }
        out.print("</td><td>");
        
        if (jas.isDisabled()) {
          out.print("<span class=\"failed\">Failed</span>");
        } else if (openForWrite) {
          EditLogOutputStream elos = jas.getCurrentStream();
          if (elos != null) {
            out.println(elos.generateHtmlReport());
          } else {
            out.println("not currently writing");
          }
        } else {
          out.println("open for read");
        }
        out.println("</td></tr>");
      }
      
      out.println("</table></div>");
    }

    void generateHealthReport(JspWriter out, NameNode nn,
        HttpServletRequest request) throws IOException {
      FSNamesystem fsn = nn.getNamesystem();
      final DatanodeManager dm = fsn.getBlockManager().getDatanodeManager();
      final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
      dm.fetchDatanodes(live, dead, true);

      int liveDecommissioned = 0;
      for (DatanodeDescriptor d : live) {
        liveDecommissioned += d.isDecommissioned() ? 1 : 0;
      }

      int deadDecommissioned = 0;
      for (DatanodeDescriptor d : dead) {
        deadDecommissioned += d.isDecommissioned() ? 1 : 0;
      }
      
      final List<DatanodeDescriptor> decommissioning = dm.getDecommissioningNodes();

      sorterField = request.getParameter("sorter/field");
      sorterOrder = request.getParameter("sorter/order");
      if (sorterField == null)
        sorterField = "name";
      if (sorterOrder == null)
        sorterOrder = "ASC";

      // Find out common suffix. Should this be before or after the sort?
      String port_suffix = null;
      if (live.size() > 0) {
        String name = live.get(0).getXferAddr();
        int idx = name.indexOf(':');
        if (idx > 0) {
          port_suffix = name.substring(idx);
        }

        for (int i = 1; port_suffix != null && i < live.size(); i++) {
          if (live.get(i).getXferAddr().endsWith(port_suffix) == false) {
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
      float percentUsed = DFSUtil.getPercentUsed(used, total);
      float percentRemaining = DFSUtil.getPercentRemaining(remaining, total);
      float median = 0;
      float max = 0;
      float min = 0;
      float dev = 0;
      
      if (live.size() > 0) {
        float totalDfsUsed = 0;
        float[] usages = new float[live.size()];
        int i = 0;
        for (DatanodeDescriptor dn : live) {
          usages[i++] = dn.getDfsUsedPercent();
          totalDfsUsed += dn.getDfsUsedPercent();
        }
        totalDfsUsed /= live.size();
        Arrays.sort(usages);
        median = usages[usages.length/2];
        max = usages[usages.length - 1];
        min = usages[0];
        
        for (i = 0; i < usages.length; i++) {
          dev += (usages[i] - totalDfsUsed) * (usages[i] - totalDfsUsed);
        }
        dev = (float) Math.sqrt(dev/usages.length);
      }

      long bpUsed = fsnStats[6];
      float percentBpUsed = DFSUtil.getPercentUsed(bpUsed, total);

      // don't show under-replicated/missing blocks or corrupt files for SBN
      // since the standby namenode doesn't compute replication queues 
      String underReplicatedBlocks = "";
      if (nn.getServiceState() == HAServiceState.ACTIVE) {
    	  underReplicatedBlocks = rowTxt() 
              + colTxt("Excludes missing blocks.")
              + "Number of Under-Replicated Blocks" + colTxt() + ":" + colTxt()
              + fsn.getBlockManager().getUnderReplicatedNotMissingBlocks(); 
      }
      out.print("<div class=\"dfstable\"> <table>\n" + rowTxt() + colTxt()
          + "Configured Capacity" + colTxt() + ":" + colTxt()
          + StringUtils.byteDesc(total) + rowTxt() + colTxt() + "DFS Used"
          + colTxt() + ":" + colTxt() + StringUtils.byteDesc(used) + rowTxt()
          + colTxt() + "Non DFS Used" + colTxt() + ":" + colTxt()
          + StringUtils.byteDesc(nonDFS) + rowTxt() + colTxt()
          + "DFS Remaining" + colTxt() + ":" + colTxt()
          + StringUtils.byteDesc(remaining) + rowTxt() + colTxt() + "DFS Used%"
          + colTxt() + ":" + colTxt()
          + percent2String(percentUsed) + rowTxt()
          + colTxt() + "DFS Remaining%" + colTxt() + ":" + colTxt()
          + percent2String(percentRemaining)
          + rowTxt() + colTxt() + "Block Pool Used" + colTxt() + ":" + colTxt()
          + StringUtils.byteDesc(bpUsed) + rowTxt()
          + colTxt() + "Block Pool Used%"+ colTxt() + ":" + colTxt()
          + percent2String(percentBpUsed) 
          + rowTxt() + colTxt() + "DataNodes usages" + colTxt() + ":" + colTxt()
          + "Min %" + colTxt() + "Median %" + colTxt() + "Max %" + colTxt()
          + "stdev %" + rowTxt() + colTxt() + colTxt() + colTxt()
          + percent2String(min)
          + colTxt() + percent2String(median)
          + colTxt() + percent2String(max)
          + colTxt() + percent2String(dev)
          + rowTxt() + colTxt()
          + "<a href=\"dfsnodelist.jsp?whatNodes=LIVE\">Live Nodes</a> "
          + colTxt() + ":" + colTxt() + live.size()
          + " (Decommissioned: " + liveDecommissioned + ")"
          + rowTxt() + colTxt()
          + "<a href=\"dfsnodelist.jsp?whatNodes=DEAD\">Dead Nodes</a> "
          + colTxt() + ":" + colTxt() + dead.size() 
          + " (Decommissioned: " + deadDecommissioned + ")"
          + rowTxt() + colTxt()
          + "<a href=\"dfsnodelist.jsp?whatNodes=DECOMMISSIONING\">"
          + "Decommissioning Nodes</a> "
          + colTxt() + ":" + colTxt() + decommissioning.size()
          + underReplicatedBlocks
          + "</table></div><br>\n");

      if (live.isEmpty() && dead.isEmpty()) {
        out.print("There are no datanodes in the cluster.");
      }
    }
  }

  static String getDelegationToken(final NamenodeProtocols nn,
      HttpServletRequest request, Configuration conf,
      final UserGroupInformation ugi) throws IOException, InterruptedException {
    Token<DelegationTokenIdentifier> token = ugi
        .doAs(new PrivilegedExceptionAction<Token<DelegationTokenIdentifier>>() {
          @Override
          public Token<DelegationTokenIdentifier> run() throws IOException {
            return nn.getDelegationToken(new Text(ugi.getUserName()));
          }
        });
    return token == null ? null : token.encodeToUrlString();
  }

  /** @return a randomly chosen datanode. */
  static DatanodeDescriptor getRandomDatanode(final NameNode namenode) {
    return (DatanodeDescriptor)namenode.getNamesystem().getBlockManager(
        ).getDatanodeManager().getNetworkTopology().chooseRandom(
        NodeBase.ROOT);
  }
  
  static void redirectToRandomDataNode(ServletContext context,
      HttpServletRequest request, HttpServletResponse resp) throws IOException,
      InterruptedException {
    final NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
    final Configuration conf = (Configuration) context
        .getAttribute(JspHelper.CURRENT_CONF);
    // We can't redirect if there isn't a DN to redirect to.
    // Lets instead show a proper error message.
    if (nn.getNamesystem().getNumLiveDataNodes() < 1) {
      throw new IOException("Can't browse the DFS since there are no " +
          "live nodes available to redirect to.");
    }
    final DatanodeID datanode = getRandomDatanode(nn);;
    UserGroupInformation ugi = JspHelper.getUGI(context, request, conf);
    String tokenString = getDelegationToken(
        nn.getRpcServer(), request, conf, ugi);
    // if the user is defined, get a delegation token and stringify it
    final String redirectLocation;
    final String nodeToRedirect;
    int redirectPort;
    if (datanode != null) {
      nodeToRedirect = datanode.getIpAddr();
      redirectPort = datanode.getInfoPort();
    } else {
      nodeToRedirect = nn.getHttpAddress().getHostName();
      redirectPort = nn.getHttpAddress().getPort();
    }

    InetSocketAddress rpcAddr = nn.getNameNodeAddress();
    String rpcHost = rpcAddr.getAddress().isAnyLocalAddress()
      ? URI.create(request.getRequestURL().toString()).getHost()
      : rpcAddr.getAddress().getHostAddress();
    String addr = rpcHost + ":" + rpcAddr.getPort();

    String fqdn = InetAddress.getByName(nodeToRedirect).getCanonicalHostName();
    redirectLocation = HttpConfig.getSchemePrefix() + fqdn + ":" + redirectPort
        + "/browseDirectory.jsp?namenodeInfoPort="
        + nn.getHttpAddress().getPort() + "&dir=/"
        + (tokenString == null ? "" :
           JspHelper.getDelegationTokenUrlParam(tokenString))
        + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, addr);
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

    private void generateNodeDataHeader(JspWriter out, DatanodeDescriptor d,
        String suffix, boolean alive, int nnHttpPort, String nnaddr)
        throws IOException {
      // from nn_browsedfscontent.jsp:
      String url = HttpConfig.getSchemePrefix() + d.getHostName() + ":"
          + d.getInfoPort()
          + "/browseDirectory.jsp?namenodeInfoPort=" + nnHttpPort + "&dir="
          + URLEncoder.encode("/", "UTF-8")
          + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnaddr);

      String name = d.getXferAddrWithHostname();
      if (!name.matches("\\d+\\.\\d+.\\d+\\.\\d+.*"))
        name = name.replaceAll("\\.[^.:]*", "");
      int idx = (suffix != null && name.endsWith(suffix)) ? name
          .indexOf(suffix) : -1;

      out.print(rowTxt() + "<td class=\"name\"><a title=\"" + d.getXferAddr()
          + "\" href=\"" + url + "\">"
          + ((idx > 0) ? name.substring(0, idx) : name) + "</a>"
          + ((alive) ? "" : "\n"));
    }

    void generateDecommissioningNodeData(JspWriter out, DatanodeDescriptor d,
        String suffix, boolean alive, int nnHttpPort, String nnaddr)
        throws IOException {
      generateNodeDataHeader(out, d, suffix, alive, nnHttpPort, nnaddr);
      if (!alive) {
        return;
      }

      long decommRequestTime = d.decommissioningStatus.getStartTime();
      long timestamp = d.getLastUpdate();
      long currentTime = Time.now();
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
    
    void generateNodeData(JspWriter out, DatanodeDescriptor d, String suffix,
        boolean alive, int nnHttpPort, String nnaddr) throws IOException {
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

      generateNodeDataHeader(out, d, suffix, alive, nnHttpPort, nnaddr);
      if (!alive) {
        out.print("<td class=\"decommissioned\"> " + 
            d.isDecommissioned() + "\n");
        return;
      }

      long c = d.getCapacity();
      long u = d.getDfsUsed();
      long nu = d.getNonDfsUsed();
      long r = d.getRemaining();
      final double percentUsedValue = d.getDfsUsedPercent();
      String percentUsed = fraction2String(percentUsedValue);
      String percentRemaining = fraction2String(d.getRemainingPercent());

      String adminState = d.getAdminState().toString();

      long timestamp = d.getLastUpdate();
      long currentTime = Time.now();
      
      long bpUsed = d.getBlockPoolUsed();
      String percentBpUsed = fraction2String(d.getBlockPoolUsedPercent());

      out.print("<td class=\"lastcontact\"> "
          + ((currentTime - timestamp) / 1000)
          + "<td class=\"adminstate\">"
          + adminState
          + "<td align=\"right\" class=\"capacity\">"
          + fraction2String(c, diskBytes)
          + "<td align=\"right\" class=\"used\">"
          + fraction2String(u, diskBytes)
          + "<td align=\"right\" class=\"nondfsused\">"
          + fraction2String(nu, diskBytes)
          + "<td align=\"right\" class=\"remaining\">"
          + fraction2String(r, diskBytes)
          + "<td align=\"right\" class=\"pcused\">"
          + percentUsed
          + "<td class=\"pcused\">"
          + ServletUtil.percentageGraph((int)percentUsedValue, 100) 
          + "<td align=\"right\" class=\"pcremaining\">"
          + percentRemaining 
          + "<td title=" + "\"blocks scheduled : "
          + d.getBlocksScheduled() + "\" class=\"blocks\">" + d.numBlocks()+"\n"
          + "<td align=\"right\" class=\"bpused\">"
          + fraction2String(bpUsed, diskBytes)
          + "<td align=\"right\" class=\"pcbpused\">"
          + percentBpUsed
          + "<td align=\"right\" class=\"volfails\">"
          + d.getVolumeFailures() + "\n");
    }

    void generateNodesList(ServletContext context, JspWriter out,
        HttpServletRequest request) throws IOException {
      final NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
      final FSNamesystem ns = nn.getNamesystem();
      final DatanodeManager dm = ns.getBlockManager().getDatanodeManager();

      final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
      dm.fetchDatanodes(live, dead, true);

      InetSocketAddress nnSocketAddress =
          (InetSocketAddress)context.getAttribute(
              NameNodeHttpServer.NAMENODE_ADDRESS_ATTRIBUTE_KEY);
      String nnaddr = nnSocketAddress.getAddress().getHostAddress() + ":"
          + nnSocketAddress.getPort();

      whatNodes = request.getParameter("whatNodes"); // show only live or only
                                                     // dead nodes
      if (null == whatNodes || whatNodes.isEmpty()) {
        out.print("Invalid input");
        return;
      }
      sorterField = request.getParameter("sorter/field");
      sorterOrder = request.getParameter("sorter/order");
      if (sorterField == null)
        sorterField = "name";
      if (sorterOrder == null)
        sorterOrder = "ASC";

      JspHelper.sortNodeList(live, sorterField, sorterOrder);

      // Find out common suffix. Should this be before or after the sort?
      String port_suffix = null;
      if (live.size() > 0) {
        String name = live.get(0).getXferAddr();
        int idx = name.indexOf(':');
        if (idx > 0) {
          port_suffix = name.substring(idx);
        }

        for (int i = 1; port_suffix != null && i < live.size(); i++) {
          if (live.get(i).getXferAddr().endsWith(port_suffix) == false) {
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
              + "<br><br>\n<table class=\"nodes\">\n");

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
                + "> Blocks <th "
                + nodeHeaderStr("bpused") + "> Block Pool<br>Used (" 
                + diskByteStr + ") <th "
                + nodeHeaderStr("pcbpused")
                + "> Block Pool<br>Used (%)"
                + "> Blocks <th " + nodeHeaderStr("volfails")
                +"> Failed Volumes\n");

            JspHelper.sortNodeList(live, sorterField, sorterOrder);
            for (int i = 0; i < live.size(); i++) {
              generateNodeData(out, live.get(i), port_suffix, true, nnHttpPort,
                  nnaddr);
            }
          }
          out.print("</table>\n");
        } else if (whatNodes.equals("DEAD")) {

          out.print("<br> <a name=\"DeadNodes\" id=\"title\"> "
              + " Dead Datanodes : " + dead.size() + "</a><br><br>\n");

          if (dead.size() > 0) {
            out.print("<table border=1 cellspacing=0> <tr id=\"row1\"> "
                + "<th " + nodeHeaderStr("node")
                + "> Node <th " + nodeHeaderStr("decommissioned")
                + "> Decommissioned\n");

            JspHelper.sortNodeList(dead, sorterField, sorterOrder);
            for (int i = 0; i < dead.size(); i++) {
              generateNodeData(out, dead.get(i), port_suffix, false,
                  nnHttpPort, nnaddr);
            }

            out.print("</table>\n");
          }
        } else if (whatNodes.equals("DECOMMISSIONING")) {
          // Decommissioning Nodes
          final List<DatanodeDescriptor> decommissioning = dm.getDecommissioningNodes();
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
                  port_suffix, true, nnHttpPort, nnaddr);
            }
            out.print("</table>\n");
          }
        } else {
          out.print("Invalid input");
        }
        out.print("</div>");
      }
    }
  }
  
  // utility class used in block_info_xml.jsp
  static class XMLBlockInfo {
    final Block block;
    final INodeFile inode;
    final BlockManager blockManager;
    
    XMLBlockInfo(FSNamesystem fsn, Long blockId) {
      this.blockManager = fsn.getBlockManager();

      if (blockId == null) {
        this.block = null;
        this.inode = null;
      } else {
        this.block = new Block(blockId);
        this.inode = (INodeFile) blockManager.getBlockCollection(block);
      }
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
          doc.pcdata(inode.getLocalParentDir());
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
          doc.pcdata(""+inode.getBlockReplication());
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
        for(final Iterator<DatanodeDescriptor> it = blockManager.datanodeIterator(block);
            it.hasNext(); ) {
          doc.startTag("replica");

          DatanodeDescriptor dd = it.next();

          doc.startTag("host_name");
          doc.pcdata(dd.getHostName());
          doc.endTag();

          boolean isCorrupt = blockManager.getCorruptReplicaBlockIds(0,
                                block.getBlockId()) != null;
          
          doc.startTag("is_corrupt");
          doc.pcdata(""+isCorrupt);
          doc.endTag();
          
          doc.endTag(); // </replica>
        }
        doc.endTag(); // </replicas>
                
      }
      
      doc.endTag(); // </block_info>
      
    }
  }
  
  // utility class used in corrupt_replicas_xml.jsp
  static class XMLCorruptBlockInfo {
    final Configuration conf;
    final Long startingBlockId;
    final int numCorruptBlocks;
    final BlockManager blockManager;
    
    XMLCorruptBlockInfo(FSNamesystem fsn, Configuration conf,
                               int numCorruptBlocks, Long startingBlockId) {
      this.blockManager = fsn.getBlockManager();
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
      
      doc.startTag(DFSConfigKeys.DFS_REPLICATION_KEY);
      doc.pcdata(""+conf.getInt(DFSConfigKeys.DFS_REPLICATION_KEY, 
                                DFSConfigKeys.DFS_REPLICATION_DEFAULT));
      doc.endTag();
      
      doc.startTag("num_missing_blocks");
      doc.pcdata(""+blockManager.getMissingBlocksCount());
      doc.endTag();
      
      doc.startTag("num_corrupt_replica_blocks");
      doc.pcdata(""+blockManager.getCorruptReplicaBlocksCount());
      doc.endTag();
     
      doc.startTag("corrupt_replica_block_ids");
      final long[] corruptBlockIds = blockManager.getCorruptReplicaBlockIds(
          numCorruptBlocks, startingBlockId);
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
