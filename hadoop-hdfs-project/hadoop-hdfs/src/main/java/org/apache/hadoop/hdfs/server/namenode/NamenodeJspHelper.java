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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Status;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
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
    if (fsn == null || !fsn.isInSafeMode())
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

  static String getRollingUpgradeText(FSNamesystem fsn) {
    if (fsn == null) {
      return "";
    }

    DatanodeManager dm = fsn.getBlockManager().getDatanodeManager();
    Map<String, Integer> list = dm.getDatanodesSoftwareVersions();
    if(list.size() > 1) {
      StringBuffer status = new StringBuffer("Rolling upgrades in progress. " +
      "There are " + list.size() + " versions of datanodes currently live: ");
      for(Map.Entry<String, Integer> ver: list.entrySet()) {
        status.append(ver.getKey() + "(" + ver.getValue() + "), ");
      }
      return status.substring(0, status.length()-2);
    }
    return "";
  }

  static String getInodeLimitText(FSNamesystem fsn) {
    if (fsn == null) {
      return "";
    }

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
        + (inodes + blocks) + " total";
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
    StringBuilder sb = new StringBuilder();
    sb.append("<div class='dfstable'><table>");
    if (fsn != null) {
      sb.append("\n  <tr><td class='col1'>Started:</td><td>" + fsn.getStartTime());
    }
    sb.append("</td></tr>\n" + "\n  <tr><td class='col1'>Version:</td><td>");
    sb.append(VersionInfo.getVersion() + ", " + VersionInfo.getRevision());
    sb.append("</td></tr>\n" + "\n  <tr><td class='col1'>Compiled:</td><td>" + VersionInfo.getDate());
    sb.append(" by " + VersionInfo.getUser() + " from " + VersionInfo.getBranch());
    if (fsn != null) {
      sb.append("</td></tr>\n  <tr><td class='col1'>Cluster ID:</td><td>" + fsn.getClusterId());
      sb.append("</td></tr>\n  <tr><td class='col1'>Block Pool ID:</td><td>" + fsn.getBlockPoolId());
    }
    sb.append("</td></tr>\n</table></div>");
    return sb.toString();
  }

  /**
   * Generate warning text if there are corrupt files.
   * @return a warning if files are corrupt, otherwise return an empty string.
   */
  static String getCorruptFilesWarning(FSNamesystem fsn) {
    if (fsn == null) {
      return "";
    }

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

  static void generateSnapshotReport(JspWriter out, FSNamesystem fsn)
      throws IOException {
    if (fsn == null) {
      return;
    }
    out.println("<div id=\"snapshotstats\"><div class=\"dfstable\">"
        + "<table class=\"storage\" title=\"Snapshot Summary\">\n"
        + "<thead><tr><td><b>Snapshottable directories</b></td>"
        + "<td><b>Snapshotted directories</b></td></tr></thead>");

    out.println(String.format("<td>%d</td><td>%d</td>", fsn.getNumSnapshottableDirs(), fsn.getNumSnapshots()));
    out.println("</table></div></div>");
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
      if (fsn == null) {
        return;
      }
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
      if (nn.getNamesystem() == null) {
        return;
      }
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
        out.print("<td>");
        
        /**
         * Insert a line break every 3 journal nodes to avoid a very wide line.
         */  
        JournalManager manager = jas.getManager();
        if (null != manager) {
          String[] managers = manager.toString().split(",");

          for (int i = 0; i < managers.length; ++i) {
            out.print(managers[i]);

            if (i < managers.length - 1) {
              out.print(",");
            }

            if ((i+1) % 3 == 0) {
              out.print("<br/>");
            }
          }

          if (jas.isRequired()) {
            out.print(" [required]");
          }
        }
        out.print("</td><td>");
        
        if (jas.isDisabled()) {
          out.print("<span class=\"failed\">Failed</span>");
        } else if (openForWrite) {
          EditLogOutputStream elos = jas.getCurrentStream();
          if (elos != null) {
            out.println(elos.generateReport());
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
      if (fsn == null) {
        return;
      }
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

    /**
     * Generates the Startup Progress report.
     * 
     * @param out JspWriter to receive output
     * @param prog StartupProgress tracking NameNode startup progress
     * @throws IOException thrown if there is an I/O error
     */
    void generateStartupProgress(JspWriter out, StartupProgress prog)
        throws IOException {
      StartupProgressView view = prog.createView();
      FormattedWriter fout = new FormattedWriter(out);
      fout.println("<div id=\"startupprogress\">");
      fout.println("<div><span>Elapsed Time:</span> %s</div>",
        StringUtils.formatTime(view.getElapsedTime()));
      fout.println("<div><span>Percent Complete:</span> %s</div>",
        StringUtils.formatPercent(view.getPercentComplete(), 2));
      fout.println("<table>");
      fout.println("<tr>");
      fout.println("<th>Phase</th>");
      fout.println("<th>Completion</th>");
      fout.println("<th>Elapsed Time</th>");
      fout.println("</tr>");
      for (Phase phase: view.getPhases()) {
        final String timeClass;
        Status status = view.getStatus(phase);
        if (status == Status.PENDING) {
          timeClass = "later";
        } else if (status == Status.RUNNING) {
          timeClass = "current";
        } else {
          timeClass = "prior";
        }

        fout.println("<tr class=\"phase %s\">", timeClass);
        printPhase(fout, view, phase);
        fout.println("</tr>");

        for (Step step: view.getSteps(phase)) {
          fout.println("<tr class=\"step %s\">", timeClass);
          printStep(fout, view, phase, step);
          fout.println("</tr>");
        }
      }
      fout.println("</table>");
      fout.println("</div>");
    }

    /**
     * Prints one line of content for a phase in the Startup Progress report.
     * 
     * @param fout FormattedWriter to receive output
     * @param view StartupProgressView containing information to print
     * @param phase Phase to print
     * @throws IOException thrown if there is an I/O error
     */
    private void printPhase(FormattedWriter fout, StartupProgressView view,
        Phase phase) throws IOException {
      StringBuilder phaseLine = new StringBuilder();
      phaseLine.append(phase.getDescription());
      String file = view.getFile(phase);
      if (file != null) {
        phaseLine.append(" ").append(file);
      }
      long size = view.getSize(phase);
      if (size != Long.MIN_VALUE) {
        phaseLine.append(" (").append(StringUtils.byteDesc(size)).append(")");
      }
      fout.println("<td class=\"startupdesc\">%s</td>", phaseLine.toString());
      fout.println("<td>%s</td>", StringUtils.formatPercent(
        view.getPercentComplete(phase), 2));
      fout.println("<td>%s</td>", view.getStatus(phase) == Status.PENDING ? "" :
        StringUtils.formatTime(view.getElapsedTime(phase)));
    }

    /**
     * Prints one line of content for a step in the Startup Progress report.
     * 
     * @param fout FormattedWriter to receive output
     * @param view StartupProgressView containing information to print
     * @param phase Phase to print
     * @param step Step to print
     * @throws IOException thrown if there is an I/O error
     */
    private void printStep(FormattedWriter fout, StartupProgressView view,
        Phase phase, Step step) throws IOException {
      StringBuilder stepLine = new StringBuilder();
      String file = step.getFile();
      if (file != null) {
        stepLine.append(file);
      }
      long size = step.getSize();
      if (size != Long.MIN_VALUE) {
        stepLine.append(" (").append(StringUtils.byteDesc(size)).append(")");
      }
      StepType type = step.getType();
      if (type != null) {
        stepLine.append(" ").append(type.getDescription());
      }

      fout.println("<td class=\"startupdesc\">%s (%d/%d)</td>",
        stepLine.toString(), view.getCount(phase, step),
        view.getTotal(phase, step));
      fout.println("<td>%s</td>", StringUtils.formatPercent(
        view.getPercentComplete(phase), 2));
      fout.println("<td>%s</td>", view.getStatus(phase) == Status.PENDING ? "" :
        StringUtils.formatTime(view.getElapsedTime(phase)));
    }

    /**
     * JspWriter wrapper that helps simplify printing formatted lines.
     */
    private static class FormattedWriter {
      private final JspWriter out;

      /**
       * Creates a new FormattedWriter that delegates to the given JspWriter.
       * 
       * @param out JspWriter to wrap
       */
      FormattedWriter(JspWriter out) {
        this.out = out;
      }

      /**
       * Prints one formatted line, followed by line terminator, using the
       * English locale.
       * 
       * @param format String format
       * @param args Object... any number of arguments to match format
       * @throws IOException thrown if there is an I/O error
       */
      void println(String format, Object... args) throws IOException {
        out.println(StringUtils.format(format, args));
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
    FSNamesystem fsn = nn.getNamesystem();

    DatanodeID datanode = null;
    if (fsn != null && fsn.getNumLiveDataNodes() >= 1) {
      datanode = getRandomDatanode(nn);
    }

    if (datanode == null) {
      throw new IOException("Can't browse the DFS since there are no " +
          "live nodes available to redirect to.");
    }

    UserGroupInformation ugi = JspHelper.getUGI(context, request, conf);
    // if the user is defined, get a delegation token and stringify it
    String tokenString = getDelegationToken(
        nn.getRpcServer(), request, conf, ugi);

    InetSocketAddress rpcAddr = nn.getNameNodeAddress();
    String rpcHost = rpcAddr.getAddress().isAnyLocalAddress()
      ? URI.create(request.getRequestURL().toString()).getHost()
      : rpcAddr.getAddress().getHostAddress();
    String addr = rpcHost + ":" + rpcAddr.getPort();

    final String redirectLocation =
        JspHelper.Url.url(request.getScheme(), datanode)
        + "/browseDirectory.jsp?namenodeInfoPort="
        + request.getServerPort() + "&dir=/"
        + (tokenString == null ? "" :
           JspHelper.getDelegationTokenUrlParam(tokenString))
        + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, addr);

    resp.sendRedirect(redirectLocation);
  }

  /**
   * Returns a descriptive label for the running NameNode.  If the NameNode has
   * initialized to the point of running its RPC server, then this label consists
   * of the host and port of the RPC server.  Otherwise, the label is a message
   * stating that the NameNode is still initializing.
   * 
   * @param nn NameNode to describe
   * @return String NameNode label
   */
  static String getNameNodeLabel(NameNode nn) {
    return nn.getRpcServer() != null ? nn.getNameNodeAddressHostPortString() :
      "initializing";
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
        String suffix, boolean alive, int nnInfoPort, String nnaddr, String scheme)
        throws IOException {
      // from nn_browsedfscontent.jsp:
      String url = "///" + JspHelper.Url.authority(scheme, d)
          + "/browseDirectory.jsp?namenodeInfoPort=" + nnInfoPort + "&dir="
          + URLEncoder.encode("/", "UTF-8")
          + JspHelper.getUrlParam(JspHelper.NAMENODE_ADDRESS, nnaddr);

      String name = d.getXferAddrWithHostname();
      if (!name.matches("\\d+\\.\\d+.\\d+\\.\\d+.*"))
        name = name.replaceAll("\\.[^.:]*", "");
      int idx = (suffix != null && name.endsWith(suffix)) ? name
          .indexOf(suffix) : -1;

      out.print(rowTxt() + "<td class=\"name\"> <a title=\"" + url
          + "\" href=\"" + url + "\">"
          + ((idx > 0) ? name.substring(0, idx) : name) + "</a>"
          + ((alive) ? "" : "\n") + "<td class=\"address\">" + d.getXferAddr());
    }

    void generateDecommissioningNodeData(JspWriter out, DatanodeDescriptor d,
        String suffix, boolean alive, int nnInfoPort, String nnaddr, String scheme)
        throws IOException {
      generateNodeDataHeader(out, d, suffix, alive, nnInfoPort, nnaddr, scheme);
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
        boolean alive, int nnInfoPort, String nnaddr, String scheme) throws IOException {
      /*
       * Say the datanode is dn1.hadoop.apache.org with ip 192.168.0.5 we use:
       * 1) d.getHostName():d.getPort() to display. Domain and port are stripped
       *    if they are common across the nodes. i.e. "dn1" 
       * 2) d.getHostName():d.getInfoPort() for url and title.
       *    i.e. "http://dn1.hadoop.apache.org:50075/..."
       * 3) d.getXferAddr() for "Transferring Address". i.e. "192.168.0.5:50010"
       * Note that "d.getHost():d.getPort()" is what DFS clients use to
       * interact with datanodes.
       */

      generateNodeDataHeader(out, d, suffix, alive, nnInfoPort, nnaddr, scheme);
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
          + d.getVolumeFailures()
          + "<td align=\"right\" class=\"version\">"
          + d.getSoftwareVersion() + "\n");
    }

    void generateNodesList(ServletContext context, JspWriter out,
        HttpServletRequest request) throws IOException {
      final NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
      final FSNamesystem ns = nn.getNamesystem();
      if (ns == null) {
        return;
      }
      final DatanodeManager dm = ns.getBlockManager().getDatanodeManager();

      final List<DatanodeDescriptor> live = new ArrayList<DatanodeDescriptor>();
      final List<DatanodeDescriptor> dead = new ArrayList<DatanodeDescriptor>();
      dm.fetchDatanodes(live, dead, true);

      String nnaddr = nn.getServiceRpcAddress().getAddress().getHostName() + ":"
          + nn.getServiceRpcAddress().getPort();

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

      if (live.isEmpty() && dead.isEmpty()) {
        out.print("There are no datanodes in the cluster");
      } else {

        int nnInfoPort = request.getServerPort();
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
                + "> Node <th " + nodeHeaderStr("address")
                + "> Transferring<br>Address <th "
                + nodeHeaderStr("lastcontact")
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
                + "> Block Pool<br>Used (%)" + " <th "
                + nodeHeaderStr("volfails")
                +"> Failed Volumes <th "
                + nodeHeaderStr("versionString")
                +"> Version\n");

            JspHelper.sortNodeList(live, sorterField, sorterOrder);
            for (int i = 0; i < live.size(); i++) {
              generateNodeData(out, live.get(i), port_suffix, true, nnInfoPort,
                  nnaddr, request.getScheme());
            }
          }
          out.print("</table>\n");
        } else if (whatNodes.equals("DEAD")) {

          out.print("<br> <a name=\"DeadNodes\" id=\"title\"> "
              + " Dead Datanodes : " + dead.size() + "</a><br><br>\n");

          if (dead.size() > 0) {
            out.print("<table border=1 cellspacing=0> <tr id=\"row1\"> "
                + "<th " + nodeHeaderStr("node")
                + "> Node <th " + nodeHeaderStr("address")
                + "> Transferring<br>Address <th "
                + nodeHeaderStr("decommissioned")
                + "> Decommissioned\n");

            JspHelper.sortNodeList(dead, sorterField, sorterOrder);
            for (int i = 0; i < dead.size(); i++) {
              generateNodeData(out, dead.get(i), port_suffix, false,
                  nnInfoPort, nnaddr, request.getScheme());
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
                + "> Node <th " + nodeHeaderStr("address")
                + "> Transferring<br>Address <th "
                + nodeHeaderStr("lastcontact")
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
                  port_suffix, true, nnInfoPort, nnaddr, request.getScheme());
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

  private static String getLocalParentDir(INode inode) {
    final INode parent = inode.isRoot() ? inode : inode.getParent();
    String parentDir = "";
    if (parent != null) {
      parentDir = parent.getFullPathName();
    }
    return (parentDir != null) ? parentDir : "";
  }

  // utility class used in block_info_xml.jsp
  static class XMLBlockInfo {
    final Block block;
    final INodeFile inode;
    final BlockManager blockManager;
    
    XMLBlockInfo(FSNamesystem fsn, Long blockId) {
      this.blockManager = fsn != null ? fsn.getBlockManager() : null;

      if (blockId == null) {
        this.block = null;
        this.inode = null;
      } else {
        this.block = new Block(blockId);
        this.inode = blockManager != null ?
          ((INode)blockManager.getBlockCollection(block)).asFile() :
          null;
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
          doc.pcdata(""+inode.getQuotaCounts().get(Quota.DISKSPACE));
          doc.endTag();

          doc.startTag("permission_status");
          doc.pcdata(inode.getPermissionStatus().toString());
          doc.endTag();

          doc.startTag("replication");
          doc.pcdata(""+inode.getFileReplication());
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
        for(DatanodeStorageInfo storage : (blockManager != null ?
                blockManager.getStorages(block) :
                Collections.<DatanodeStorageInfo>emptyList())) {
          doc.startTag("replica");

          DatanodeDescriptor dd = storage.getDatanodeDescriptor();

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
      this.blockManager = fsn != null ? fsn.getBlockManager() : null;
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
      doc.pcdata("" + (blockManager != null ?
        blockManager.getMissingBlocksCount() : 0));
      doc.endTag();
      
      doc.startTag("num_corrupt_replica_blocks");
      doc.pcdata("" + (blockManager != null ?
        blockManager.getCorruptReplicaBlocksCount() : 0));
      doc.endTag();
     
      doc.startTag("corrupt_replica_block_ids");
      final long[] corruptBlockIds = blockManager != null ?
        blockManager.getCorruptReplicaBlockIds(numCorruptBlocks,
        startingBlockId) : null;
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
