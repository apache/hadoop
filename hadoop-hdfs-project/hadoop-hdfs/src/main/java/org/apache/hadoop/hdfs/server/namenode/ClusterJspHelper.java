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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.MalformedObjectNameException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DFSUtil.ConfiguredNNAddress;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.znerd.xmlenc.XMLOutputter;

import com.google.common.base.Charsets;

/**
 * This class generates the data that is needed to be displayed on cluster web 
 * console.
 */
@InterfaceAudience.Private
class ClusterJspHelper {
  private static final Log LOG = LogFactory.getLog(ClusterJspHelper.class);
  public static final String OVERALL_STATUS = "overall-status";
  public static final String DEAD = "Dead";
  private static final String JMX_QRY = 
    "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo";
  
  /**
   * JSP helper function that generates cluster health report.  When 
   * encountering exception while getting Namenode status, the exception will 
   * be listed on the page with corresponding stack trace.
   */
  ClusterStatus generateClusterHealthReport() {
    ClusterStatus cs = new ClusterStatus();
    Configuration conf = new Configuration();
    List<ConfiguredNNAddress> nns = null;
    try {
      nns = DFSUtil.flattenAddressMap(
          DFSUtil.getNNServiceRpcAddresses(conf));
    } catch (Exception e) {
      // Could not build cluster status
      cs.setError(e);
      return cs;
    }
    
    // Process each namenode and add it to ClusterStatus
    for (ConfiguredNNAddress cnn : nns) {
      InetSocketAddress isa = cnn.getAddress();
      NamenodeMXBeanHelper nnHelper = null;
      try {
        nnHelper = new NamenodeMXBeanHelper(isa, conf);
        String mbeanProps= queryMbean(nnHelper.httpAddress, conf);
        NamenodeStatus nn = nnHelper.getNamenodeStatus(mbeanProps);
        if (cs.clusterid.isEmpty() || cs.clusterid.equals("")) { // Set clusterid only once
          cs.clusterid = nnHelper.getClusterId(mbeanProps);
        }
        cs.addNamenodeStatus(nn);
      } catch ( Exception e ) {
        // track exceptions encountered when connecting to namenodes
        cs.addException(isa.getHostName(), e);
        continue;
      } 
    }
    return cs;
  }

  /**
   * Helper function that generates the decommissioning report.  Connect to each
   * Namenode over http via JmxJsonServlet to collect the data nodes status.
   */
  DecommissionStatus generateDecommissioningReport() {
    String clusterid = "";
    Configuration conf = new Configuration();
    List<ConfiguredNNAddress> cnns = null;
    try {
      cnns = DFSUtil.flattenAddressMap(
          DFSUtil.getNNServiceRpcAddresses(conf));
    } catch (Exception e) {
      // catch any exception encountered other than connecting to namenodes
      DecommissionStatus dInfo = new DecommissionStatus(clusterid, e);
      return dInfo;
    }
    
    // Outer map key is datanode. Inner map key is namenode and the value is 
    // decom status of the datanode for the corresponding namenode
    Map<String, Map<String, String>> statusMap = 
      new HashMap<String, Map<String, String>>();
    
    // Map of exceptions encountered when connecting to namenode
    // key is namenode and value is exception
    Map<String, Exception> decommissionExceptions = 
      new HashMap<String, Exception>();
    
    List<String> unreportedNamenode = new ArrayList<String>();
    for (ConfiguredNNAddress cnn : cnns) {
      InetSocketAddress isa = cnn.getAddress();
      NamenodeMXBeanHelper nnHelper = null;
      try {
        nnHelper = new NamenodeMXBeanHelper(isa, conf);
        String mbeanProps= queryMbean(nnHelper.httpAddress, conf);
        if (clusterid.equals("")) {
          clusterid = nnHelper.getClusterId(mbeanProps);
        }
        nnHelper.getDecomNodeInfoForReport(statusMap, mbeanProps);
      } catch (Exception e) {
        // catch exceptions encountered while connecting to namenodes
        String nnHost = isa.getHostName();
        decommissionExceptions.put(nnHost, e);
        unreportedNamenode.add(nnHost);
        continue;
      } 
    }
    updateUnknownStatus(statusMap, unreportedNamenode);
    getDecommissionNodeClusterState(statusMap);
    return new DecommissionStatus(statusMap, clusterid,
        getDatanodeHttpPort(conf), decommissionExceptions);
  }
  
  /**
   * Based on the state of the datanode at each namenode, marks the overall
   * state of the datanode across all the namenodes, to one of the following:
   * <ol>
   * <li>{@link DecommissionStates#DECOMMISSIONED}</li>
   * <li>{@link DecommissionStates#DECOMMISSION_INPROGRESS}</li>
   * <li>{@link DecommissionStates#PARTIALLY_DECOMMISSIONED}</li>
   * <li>{@link DecommissionStates#UNKNOWN}</li>
   * </ol>
   * 
   * @param statusMap
   *          map whose key is datanode, value is an inner map with key being
   *          namenode, value being decommission state.
   */
  private void getDecommissionNodeClusterState(
      Map<String, Map<String, String>> statusMap) {
    if (statusMap == null || statusMap.isEmpty()) {
      return;
    }
    
    // For each datanodes
    Iterator<Entry<String, Map<String, String>>> it = 
      statusMap.entrySet().iterator();
    while (it.hasNext()) {
      // Map entry for a datanode:
      // key is namenode, value is datanode status at the namenode
      Entry<String, Map<String, String>> entry = it.next();
      Map<String, String> nnStatus = entry.getValue();
      if (nnStatus == null || nnStatus.isEmpty()) {
        continue;
      }
      
      boolean isUnknown = false;
      int unknown = 0;
      int decommissioned = 0;
      int decomInProg = 0;
      int inservice = 0;
      int dead = 0;
      DecommissionStates overallState = DecommissionStates.UNKNOWN;
      // Process a datanode state from each namenode
      for (Map.Entry<String, String> m : nnStatus.entrySet()) {
        String status = m.getValue();
        if (status.equals(DecommissionStates.UNKNOWN.toString())) {
          isUnknown = true;
          unknown++;
        } else 
          if (status.equals(AdminStates.DECOMMISSION_INPROGRESS.toString())) {
          decomInProg++;
        } else if (status.equals(AdminStates.DECOMMISSIONED.toString())) {
          decommissioned++;
        } else if (status.equals(AdminStates.NORMAL.toString())) {
          inservice++;
        } else if (status.equals(DEAD)) {
          // dead
          dead++;
        }
      }
      
      // Consolidate all the states from namenode in to overall state
      int nns = nnStatus.keySet().size();
      if ((inservice + dead + unknown) == nns) {
        // Do not display this data node. Remove this entry from status map.  
        it.remove();
      } else if (isUnknown) {
        overallState = DecommissionStates.UNKNOWN;
      } else if (decommissioned == nns) {
        overallState = DecommissionStates.DECOMMISSIONED;
      } else if ((decommissioned + decomInProg) == nns) {
        overallState = DecommissionStates.DECOMMISSION_INPROGRESS;
      } else if ((decommissioned + decomInProg < nns) 
        && (decommissioned + decomInProg > 0)){
        overallState = DecommissionStates.PARTIALLY_DECOMMISSIONED;
      } else {
        LOG.warn("Cluster console encounters a not handled situtation.");
      }
        
      // insert overall state
      nnStatus.put(OVERALL_STATUS, overallState.toString());
    }
  }

  /**
   * update unknown status in datanode status map for every unreported namenode
   */
  private void updateUnknownStatus(Map<String, Map<String, String>> statusMap,
      List<String> unreportedNn) {
    if (unreportedNn == null || unreportedNn.isEmpty()) {
      // no unreported namenodes
      return;
    }
    
    for (Map.Entry<String, Map<String,String>> entry : statusMap.entrySet()) {
      String dn = entry.getKey();
      Map<String, String> nnStatus = entry.getValue();
      for (String nn : unreportedNn) {
        nnStatus.put(nn, DecommissionStates.UNKNOWN.toString());
      }
      statusMap.put(dn, nnStatus);
    }
  }

  /**
   * Get datanode http port from configration
   */
  private int getDatanodeHttpPort(Configuration conf) {
    String address = conf.get(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "");
    if (address.equals("")) {
      return -1;
    }
    return Integer.parseInt(address.split(":")[1]);
  }
  
  /**
   * Class for connecting to Namenode over http via JmxJsonServlet 
   * to get JMX attributes exposed by the MXBean.  
   */
  static class NamenodeMXBeanHelper {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final String host;
    private final URI httpAddress;
    
    NamenodeMXBeanHelper(InetSocketAddress addr, Configuration conf)
        throws IOException, MalformedObjectNameException {
      this.host = addr.getHostName();
      this.httpAddress = DFSUtil.getInfoServer(addr, conf,
          DFSUtil.getHttpClientScheme(conf));
    }
    
    /** Get the map corresponding to the JSON string */
    private static Map<String, Map<String, Object>> getNodeMap(String json)
        throws IOException {
      TypeReference<Map<String, Map<String, Object>>> type = 
        new TypeReference<Map<String, Map<String, Object>>>() { };
      return mapper.readValue(json, type);
    }
    
    /**
     * Get the number of live datanodes.
     * 
     * @param json JSON string that contains live node status.
     * @param nn namenode status to return information in
     */
    private static void getLiveNodeCount(String json, NamenodeStatus nn)
        throws IOException {
      // Map of datanode host to (map of attribute name to value)
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap == null || nodeMap.isEmpty()) {
        return;
      }
      
      nn.liveDatanodeCount = nodeMap.size();
      for (Entry<String, Map<String, Object>> entry : nodeMap.entrySet()) {
        // Inner map of attribute name to value
        Map<String, Object> innerMap = entry.getValue();
        if (innerMap != null) {
          if (innerMap.get("adminState")
              .equals(AdminStates.DECOMMISSIONED.toString())) {
            nn.liveDecomCount++;
          }
        }
      }
    }
  
    /**
     * Count the number of dead datanode.
     * 
     * @param nn namenode
     * @param json JSON string
     */
    private static void getDeadNodeCount(String json, NamenodeStatus nn)
        throws IOException {
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap == null || nodeMap.isEmpty()) {
        return;
      }
      
      nn.deadDatanodeCount = nodeMap.size();
      for (Entry<String, Map<String, Object>> entry : nodeMap.entrySet()) {
        Map<String, Object> innerMap = entry.getValue();
        if (innerMap != null && !innerMap.isEmpty()) {
          if (((Boolean) innerMap.get("decommissioned"))
              .booleanValue() == true) {
            nn.deadDecomCount++;
          }
        }
      }
    }
  
    public String getClusterId(String props) throws IOException {
      return getProperty(props, "ClusterId").getTextValue();
    }
    
    public NamenodeStatus getNamenodeStatus(String props) throws IOException,
        MalformedObjectNameException, NumberFormatException {
      NamenodeStatus nn = new NamenodeStatus();
      nn.host = host;
      nn.filesAndDirectories = getProperty(props, "TotalFiles").getLongValue();
      nn.capacity = getProperty(props, "Total").getLongValue();
      nn.free = getProperty(props, "Free").getLongValue();
      nn.bpUsed = getProperty(props, "BlockPoolUsedSpace").getLongValue();
      nn.nonDfsUsed = getProperty(props, "NonDfsUsedSpace").getLongValue();
      nn.blocksCount = getProperty(props, "TotalBlocks").getLongValue();
      nn.missingBlocksCount = getProperty(props, "NumberOfMissingBlocks")
          .getLongValue();
      nn.httpAddress = httpAddress.toURL();
      getLiveNodeCount(getProperty(props, "LiveNodes").asText(), nn);
      getDeadNodeCount(getProperty(props, "DeadNodes").asText(), nn);
      nn.softwareVersion = getProperty(props, "SoftwareVersion").getTextValue();
      return nn;
    }
    
    /**
     * Get the decommission node information.
     * @param statusMap data node status map
     * @param props string
     */
    private void getDecomNodeInfoForReport(
        Map<String, Map<String, String>> statusMap, String props)
        throws IOException, MalformedObjectNameException {
      getLiveNodeStatus(statusMap, host, getProperty(props, "LiveNodes")
          .asText());
      getDeadNodeStatus(statusMap, host, getProperty(props, "DeadNodes")
          .asText());
      getDecommissionNodeStatus(statusMap, host,
          getProperty(props, "DecomNodes").asText());
    }
  
    /**
     * Store the live datanode status information into datanode status map and
     * DecommissionNode.
     * 
     * @param statusMap Map of datanode status. Key is datanode, value
     *          is an inner map whose key is namenode, value is datanode status.
     *          reported by each namenode.
     * @param namenodeHost host name of the namenode
     * @param json JSON string contains datanode status
     * @throws IOException
     */
    private static void getLiveNodeStatus(
        Map<String, Map<String, String>> statusMap, String namenodeHost,
        String json) throws IOException {
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap != null && !nodeMap.isEmpty()) {
        List<String> liveDecommed = new ArrayList<String>();
        for (Map.Entry<String, Map<String, Object>> entry: nodeMap.entrySet()) {
          Map<String, Object> innerMap = entry.getValue();
          String dn = entry.getKey();
          if (innerMap != null) {
            if (innerMap.get("adminState").equals(
                AdminStates.DECOMMISSIONED.toString())) {
              liveDecommed.add(dn);
            }
            // the inner map key is namenode, value is datanode status.
            Map<String, String> nnStatus = statusMap.get(dn);
            if (nnStatus == null) {
              nnStatus = new HashMap<String, String>();
            }
            nnStatus.put(namenodeHost, (String) innerMap.get("adminState"));
            // map whose key is datanode, value is the inner map.
            statusMap.put(dn, nnStatus);
          }
        }
      }
    }
  
    /**
     * Store the dead datanode information into datanode status map and
     * DecommissionNode.
     * 
     * @param statusMap map with key being datanode, value being an
     *          inner map (key:namenode, value:decommisionning state).
     * @param host datanode hostname
     * @param json String
     * @throws IOException
     */
    private static void getDeadNodeStatus(
        Map<String, Map<String, String>> statusMap, String host,
        String json) throws IOException {
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap == null || nodeMap.isEmpty()) {
        return;
      }
      List<String> deadDn = new ArrayList<String>();
      List<String> deadDecommed = new ArrayList<String>();
      for (Entry<String, Map<String, Object>> entry : nodeMap.entrySet()) {
        deadDn.add(entry.getKey());
        Map<String, Object> deadNodeDetailMap = entry.getValue();
        String dn = entry.getKey();
        if (deadNodeDetailMap != null && !deadNodeDetailMap.isEmpty()) {
          // NN - status
          Map<String, String> nnStatus = statusMap.get(dn);
          if (nnStatus == null) {
            nnStatus = new HashMap<String, String>();
          }
          if (((Boolean) deadNodeDetailMap.get("decommissioned"))
              .booleanValue() == true) {
            deadDecommed.add(dn);
            nnStatus.put(host, AdminStates.DECOMMISSIONED.toString());
          } else {
            nnStatus.put(host, DEAD);
          }
          // dn-nn-status
          statusMap.put(dn, nnStatus);
        }
      }
    }
  
    /**
     * Get the decommisioning datanode information.
     * 
     * @param dataNodeStatusMap map with key being datanode, value being an
     *          inner map (key:namenode, value:decommisionning state).
     * @param host datanode
     * @param json String
     */
    private static void getDecommissionNodeStatus(
        Map<String, Map<String, String>> dataNodeStatusMap, String host,
        String json) throws IOException {
      Map<String, Map<String, Object>> nodeMap = getNodeMap(json);
      if (nodeMap == null || nodeMap.isEmpty()) {
        return;
      }
      List<String> decomming = new ArrayList<String>();
      for (Entry<String, Map<String, Object>> entry : nodeMap.entrySet()) {
        String dn = entry.getKey();
        decomming.add(dn);
        // nn-status
        Map<String, String> nnStatus = new HashMap<String, String>();
        if (dataNodeStatusMap.containsKey(dn)) {
          nnStatus = dataNodeStatusMap.get(dn);
        }
        nnStatus.put(host, AdminStates.DECOMMISSION_INPROGRESS.toString());
        // dn-nn-status
        dataNodeStatusMap.put(dn, nnStatus);
      }
    }
  }

  /**
   * This class contains cluster statistics.
   */
  static class ClusterStatus {
    /** Exception indicates failure to get cluster status */
    Exception error = null;
    
    /** Cluster status information */
    String clusterid = "";
    long total_sum = 0;
    long free_sum = 0;
    long clusterDfsUsed = 0;
    long nonDfsUsed_sum = 0;
    long totalFilesAndDirectories = 0;
    
    /** List of namenodes in the cluster */
    final List<NamenodeStatus> nnList = new ArrayList<NamenodeStatus>();
    
    /** Map of namenode host and exception encountered when getting status */
    final Map<String, Exception> nnExceptions = new HashMap<String, Exception>();
    
    public void setError(Exception e) {
      error = e;
    }
    
    public void addNamenodeStatus(NamenodeStatus nn) {
      nnList.add(nn);
      
      // Add namenode status to cluster status
      totalFilesAndDirectories += nn.filesAndDirectories;
      total_sum += nn.capacity;
      free_sum += nn.free;
      clusterDfsUsed += nn.bpUsed;
      nonDfsUsed_sum += nn.nonDfsUsed;
    }

    public void addException(String host, Exception e) {
      nnExceptions.put(host, e);
    }

    public void toXML(XMLOutputter doc) throws IOException {
      if (error != null) {
        // general exception, only print exception message onto web page.
        createGeneralException(doc, clusterid,
            StringUtils.stringifyException(error));
        doc.getWriter().flush();
        return;
      }
      
      int size = nnList.size();
      long total = 0L, free = 0L, nonDfsUsed = 0l;
      float dfsUsedPercent = 0.0f, dfsRemainingPercent = 0.0f;
      if (size > 0) {
        total = total_sum / size;
        free = free_sum / size;
        nonDfsUsed = nonDfsUsed_sum / size;
        dfsUsedPercent = DFSUtil.getPercentUsed(clusterDfsUsed, total);
        dfsRemainingPercent = DFSUtil.getPercentRemaining(free, total);
      }
    
      doc.startTag("cluster");
      doc.attribute("clusterId", clusterid);
    
      doc.startTag("storage");
    
      toXmlItemBlock(doc, "Total Files And Directories",
          Long.toString(totalFilesAndDirectories));
    
      toXmlItemBlock(doc, "Configured Capacity", StringUtils.byteDesc(total));
    
      toXmlItemBlock(doc, "DFS Used", StringUtils.byteDesc(clusterDfsUsed));
    
      toXmlItemBlock(doc, "Non DFS Used", StringUtils.byteDesc(nonDfsUsed));
    
      toXmlItemBlock(doc, "DFS Remaining", StringUtils.byteDesc(free));
    
      // dfsUsedPercent
      toXmlItemBlock(doc, "DFS Used%", DFSUtil.percent2String(dfsUsedPercent));
    
      // dfsRemainingPercent
      toXmlItemBlock(doc, "DFS Remaining%", DFSUtil.percent2String(dfsRemainingPercent));
    
      doc.endTag(); // storage
    
      doc.startTag("namenodes");
      // number of namenodes
      toXmlItemBlock(doc, "NamenodesCount", Integer.toString(size));
    
      for (NamenodeStatus nn : nnList) {
        doc.startTag("node");
        toXmlItemBlockWithLink(doc, nn.host, nn.httpAddress, "NameNode");
        toXmlItemBlock(doc, "Blockpool Used",
            StringUtils.byteDesc(nn.bpUsed));
        toXmlItemBlock(doc, "Blockpool Used%",
            DFSUtil.percent2String(DFSUtil.getPercentUsed(nn.bpUsed, total)));
        toXmlItemBlock(doc, "Files And Directories",
            Long.toString(nn.filesAndDirectories));
        toXmlItemBlock(doc, "Blocks", Long.toString(nn.blocksCount));
        toXmlItemBlock(doc, "Missing Blocks",
            Long.toString(nn.missingBlocksCount));
        toXmlItemBlockWithLink(doc, nn.liveDatanodeCount + " ("
            + nn.liveDecomCount + ")", new URL(nn.httpAddress,
            "/dfsnodelist.jsp?whatNodes=LIVE"),
            "Live Datanode (Decommissioned)");
        toXmlItemBlockWithLink(doc, nn.deadDatanodeCount + " ("
            + nn.deadDecomCount + ")", new URL(nn.httpAddress,
            "/dfsnodelist.jsp?whatNodes=DEAD"),
            "Dead Datanode (Decommissioned)");
        toXmlItemBlock(doc, "Software Version", nn.softwareVersion);
        doc.endTag(); // node
      }
      doc.endTag(); // namenodes
    
      createNamenodeExceptionMsg(doc, nnExceptions);
      doc.endTag(); // cluster
      doc.getWriter().flush();
    }
  }
  
  /**
   * This class stores namenode statistics to be used to generate cluster
   * web console report.
   */
  static class NamenodeStatus {
    String host = "";
    long capacity = 0L;
    long free = 0L;
    long bpUsed = 0L;
    long nonDfsUsed = 0L;
    long filesAndDirectories = 0L;
    long blocksCount = 0L;
    long missingBlocksCount = 0L;
    int liveDatanodeCount = 0;
    int liveDecomCount = 0;
    int deadDatanodeCount = 0;
    int deadDecomCount = 0;
    URL httpAddress = null;
    String softwareVersion = "";
  }

  /**
   * cluster-wide decommission state of a datanode
   */
  public enum DecommissionStates {
    /*
     * If datanode state is decommissioning at one or more namenodes and 
     * decommissioned at the rest of the namenodes.
     */
    DECOMMISSION_INPROGRESS("Decommission In Progress"),
    
    /* If datanode state at all the namenodes is decommissioned */
    DECOMMISSIONED("Decommissioned"),
    
    /*
     * If datanode state is not decommissioning at one or more namenodes and 
     * decommissioned/decommissioning at the rest of the namenodes.
     */
    PARTIALLY_DECOMMISSIONED("Partially Decommissioning"),
    
    /*
     * If datanode state is not known at a namenode, due to problems in getting
     * the datanode state from the namenode.
     */
    UNKNOWN("Unknown");

    final String value;
    
    DecommissionStates(final String v) {
      this.value = v;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  /**
   * This class consolidates the decommissioning datanodes information in the
   * cluster and generates decommissioning reports in XML.
   */
  static class DecommissionStatus {
    /** Error when set indicates failure to get decomission status*/
    final Exception error;
    
    /** Map of dn host <-> (Map of NN host <-> decommissioning state) */
    final Map<String, Map<String, String>> statusMap;
    final String clusterid;
    final int httpPort;
    int decommissioned = 0;   // total number of decommissioned nodes
    int decommissioning = 0;  // total number of decommissioning datanodes
    int partial = 0;          // total number of partially decommissioned nodes
    
    /** Map of namenode and exception encountered when getting decom status */
    Map<String, Exception> exceptions = new HashMap<String, Exception>();

    private DecommissionStatus(Map<String, Map<String, String>> statusMap,
        String cid, int httpPort, Map<String, Exception> exceptions) {
      this(statusMap, cid, httpPort, exceptions, null);
    }

    public DecommissionStatus(String cid, Exception e) {
      this(null, cid, -1, null, e);
    }
    
    private DecommissionStatus(Map<String, Map<String, String>> statusMap,
        String cid, int httpPort, Map<String, Exception> exceptions,
        Exception error) {
      this.statusMap = statusMap;
      this.clusterid = cid;
      this.httpPort = httpPort;
      this.exceptions = exceptions;
      this.error = error;
    }

    /**
     * Generate decommissioning datanode report in XML format
     * 
     * @param doc
     *          , xmloutputter
     * @throws IOException
     */
    public void toXML(XMLOutputter doc) throws IOException {
      if (error != null) {
        createGeneralException(doc, clusterid,
            StringUtils.stringifyException(error));
        doc.getWriter().flush();
        return;
      } 
      if (statusMap == null || statusMap.isEmpty()) {
        // none of the namenodes has reported, print exceptions from each nn.
        doc.startTag("cluster");
        createNamenodeExceptionMsg(doc, exceptions);
        doc.endTag();
        doc.getWriter().flush();
        return;
      }
      doc.startTag("cluster");
      doc.attribute("clusterId", clusterid);

      doc.startTag("decommissioningReport");
      countDecommissionDatanodes();
      toXmlItemBlock(doc, DecommissionStates.DECOMMISSIONED.toString(),
          Integer.toString(decommissioned));

      toXmlItemBlock(doc,
          DecommissionStates.DECOMMISSION_INPROGRESS.toString(),
          Integer.toString(decommissioning));

      toXmlItemBlock(doc,
          DecommissionStates.PARTIALLY_DECOMMISSIONED.toString(),
          Integer.toString(partial));

      doc.endTag(); // decommissioningReport

      doc.startTag("datanodes");
      Set<String> dnSet = statusMap.keySet();
      for (String dnhost : dnSet) {
        Map<String, String> nnStatus = statusMap.get(dnhost);
        if (nnStatus == null || nnStatus.isEmpty()) {
          continue;
        }
        String overallStatus = nnStatus.get(OVERALL_STATUS);
        // check if datanode is in decommission states
        if (overallStatus != null
            && (overallStatus.equals(AdminStates.DECOMMISSION_INPROGRESS
                .toString())
                || overallStatus.equals(AdminStates.DECOMMISSIONED.toString())
                || overallStatus
                    .equals(DecommissionStates.PARTIALLY_DECOMMISSIONED
                        .toString()) || overallStatus
                .equals(DecommissionStates.UNKNOWN.toString()))) {
          doc.startTag("node");
          // dn
          toXmlItemBlockWithLink(doc, dnhost, new URL("http", dnhost, httpPort,
              ""), "DataNode");

          // overall status first
          toXmlItemBlock(doc, OVERALL_STATUS, overallStatus);

          for (Map.Entry<String, String> m : nnStatus.entrySet()) {
            String nn = m.getKey();
            if (nn.equals(OVERALL_STATUS)) {
              continue;
            }
            // xml
            toXmlItemBlock(doc, nn, nnStatus.get(nn));
          }
          doc.endTag(); // node
        }
      }
      doc.endTag(); // datanodes

      createNamenodeExceptionMsg(doc, exceptions);

      doc.endTag();// cluster
    } // toXML

    /**
     * Count the total number of decommissioned/decommission_inprogress/
     * partially decommissioned datanodes.
     */
    private void countDecommissionDatanodes() {
      for (String dn : statusMap.keySet()) {
        Map<String, String> nnStatus = statusMap.get(dn);
        String status = nnStatus.get(OVERALL_STATUS);
        if (status.equals(DecommissionStates.DECOMMISSIONED.toString())) {
          decommissioned++;
        } else if (status.equals(DecommissionStates.DECOMMISSION_INPROGRESS
            .toString())) {
          decommissioning++;
        } else if (status.equals(DecommissionStates.PARTIALLY_DECOMMISSIONED
            .toString())) {
          partial++;
        }
      }
    }
  }

  /**
   * Generate a XML block as such, <item label=key value=value/>
   */
  private static void toXmlItemBlock(XMLOutputter doc, String key, String value)
      throws IOException {
    doc.startTag("item");
    doc.attribute("label", key);
    doc.attribute("value", value);
    doc.endTag();
  }

  /**
   * Generate a XML block as such, <item label="Node" value="hostname"
   * link="http://hostname:50070" />
   */
  private static void toXmlItemBlockWithLink(XMLOutputter doc, String value,
      URL url, String label) throws IOException {
    doc.startTag("item");
    doc.attribute("label", label);
    doc.attribute("value", value);
    doc.attribute("link", url.toString());
    doc.endTag(); // item
  }

  /**
   * create the XML for exceptions that we encountered when connecting to
   * namenode.
   */
  private static void createNamenodeExceptionMsg(XMLOutputter doc,
      Map<String, Exception> exceptionMsg) throws IOException {
    if (exceptionMsg.size() > 0) {
      doc.startTag("unreportedNamenodes");
      for (Map.Entry<String, Exception> m : exceptionMsg.entrySet()) {
        doc.startTag("node");
        doc.attribute("name", m.getKey());
        doc.attribute("exception",
            StringUtils.stringifyException(m.getValue()));
        doc.endTag();// node
      }
      doc.endTag(); // unreportedNamnodes
    }
  }

  /**
   * create XML block from general exception.
   */
  private static void createGeneralException(XMLOutputter doc,
      String clusterid, String eMsg) throws IOException {
    doc.startTag("cluster");
    doc.attribute("clusterId", clusterid);
    doc.startTag("message");
    doc.startTag("item");
    doc.attribute("msg", eMsg);
    doc.endTag(); // item
    doc.endTag(); // message
    doc.endTag(); // cluster
  }
  
  /**
   * Read in the content from a URL
   * @param url URL To read
   * @return the text from the output
   * @throws IOException if something went wrong
   */
  private static String readOutput(URL url) throws IOException {
    StringBuilder out = new StringBuilder();
    URLConnection connection = url.openConnection();
    BufferedReader in = new BufferedReader(
                            new InputStreamReader(
                            connection.getInputStream(), Charsets.UTF_8));
    String inputLine;
    while ((inputLine = in.readLine()) != null) {
      out.append(inputLine);
    }
    in.close();
    return out.toString();
  }

  private static String queryMbean(URI httpAddress, Configuration conf)
    throws IOException {
    /**
     * Although the other namenode might support HTTPS, it is fundamentally
     * broken to get the JMX via an HTTPS connection inside the namenode,
     * because in HTTPS set up the principal of the client and the one of
     * the namenode differs. Therefore, there is no guarantees that the
     * HTTPS connection can be set up.
     *
     * As a result, we just hard code the connection as an HTTP connection.
     */
    URL url = new URL(httpAddress.toURL(), JMX_QRY);
    return readOutput(url);
  }
  /**
   * In order to query a namenode mxbean, a http connection in the form of
   * "http://hostname/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
   * is sent to namenode.  JMX attributes are exposed via JmxJsonServelet on 
   * the namenode side.
   */
  private static JsonNode getProperty(String props, String propertyname)
  throws IOException {
    if (props == null || props.equals("") || propertyname == null 
        || propertyname.equals("")) {
      return null;
    }
    ObjectMapper m = new ObjectMapper();
    JsonNode rootNode = m.readValue(props, JsonNode.class);
    JsonNode jn = rootNode.get("beans").get(0).get(propertyname);
    return jn;
  }
} 
