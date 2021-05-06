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
package org.apache.hadoop.tools.dynamometer;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.Logger;


/**
 * A collection of utilities used by the Dynamometer infrastructure application.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DynoInfraUtils {

  private DynoInfraUtils() {}

  public static final String DYNO_CONF_PREFIX = "dyno.";
  public static final String DYNO_INFRA_PREFIX = DYNO_CONF_PREFIX + "infra.";

  public static final String APACHE_DOWNLOAD_MIRROR_KEY = DYNO_CONF_PREFIX
      + "apache-mirror";
  // Set a generic mirror as the default.
  public static final String APACHE_DOWNLOAD_MIRROR_DEFAULT =
      "http://mirrors.ocf.berkeley.edu/apache/";
  private static final String APACHE_DOWNLOAD_MIRROR_SUFFIX_FORMAT =
      "hadoop/common/hadoop-%s/hadoop-%s.tar.gz";
  public static final String HADOOP_TAR_FILENAME_FORMAT = "hadoop-%s.tar.gz";

  public static final String DATANODE_LIVE_MIN_FRACTION_KEY =
      DYNO_INFRA_PREFIX + "ready.datanode-min-fraction";
  public static final float DATANODE_LIVE_MIN_FRACTION_DEFAULT = 0.99f;
  public static final String MISSING_BLOCKS_MAX_FRACTION_KEY =
      DYNO_INFRA_PREFIX + "ready.missing-blocks-max-fraction";
  public static final float MISSING_BLOCKS_MAX_FRACTION_DEFAULT = 0.0001f;
  public static final String UNDERREPLICATED_BLOCKS_MAX_FRACTION_KEY =
      DYNO_INFRA_PREFIX + "ready.underreplicated-blocks-max-fraction";
  public static final float UNDERREPLICATED_BLOCKS_MAX_FRACTION_DEFAULT = 0.01f;

  // The JMX bean queries to execute for various beans.
  public static final String NAMENODE_STARTUP_PROGRESS_JMX_QUERY =
      "Hadoop:service=NameNode,name=StartupProgress";
  public static final String FSNAMESYSTEM_JMX_QUERY =
      "Hadoop:service=NameNode,name=FSNamesystem";
  public static final String FSNAMESYSTEM_STATE_JMX_QUERY =
      "Hadoop:service=NameNode,name=FSNamesystemState";
  public static final String NAMENODE_INFO_JMX_QUERY =
      "Hadoop:service=NameNode,name=NameNodeInfo";
  // The JMX property names of various properties.
  public static final String JMX_MISSING_BLOCKS = "MissingBlocks";
  public static final String JMX_UNDER_REPLICATED_BLOCKS =
      "UnderReplicatedBlocks";
  public static final String JMX_BLOCKS_TOTAL = "BlocksTotal";
  public static final String JMX_LIVE_NODE_COUNT = "NumLiveDataNodes";
  public static final String JMX_LIVE_NODES_LIST = "LiveNodes";

  /**
   * If a file matching {@value HADOOP_TAR_FILENAME_FORMAT} and {@code version}
   * is found in {@code destinationDir}, return its path. Otherwise, first
   * download the tarball from an Apache mirror. If the
   * {@value APACHE_DOWNLOAD_MIRROR_KEY} configuration or system property
   * (checked in that order) is set, use that as the mirror; else use
   * {@value APACHE_DOWNLOAD_MIRROR_DEFAULT}.
   *
   * @param destinationDir destination directory to save a tarball
   * @param version The version of Hadoop to download, like "2.7.4"
   *                or "3.0.0-beta1"
   * @param conf configuration
   * @param log logger instance
   * @return The path to the tarball.
   * @throws IOException on failure
   */
  public static File fetchHadoopTarball(File destinationDir, String version,
      Configuration conf, Logger log) throws IOException {
    log.info("Looking for Hadoop tarball for version: " + version);
    File destinationFile = new File(destinationDir,
        String.format(HADOOP_TAR_FILENAME_FORMAT, version));
    if (destinationFile.exists()) {
      log.info("Found tarball at: " + destinationFile.getAbsolutePath());
      return destinationFile;
    }
    String apacheMirror = conf.get(APACHE_DOWNLOAD_MIRROR_KEY);
    if (apacheMirror == null) {
      apacheMirror = System.getProperty(APACHE_DOWNLOAD_MIRROR_KEY,
          APACHE_DOWNLOAD_MIRROR_DEFAULT);
    }

    if (!destinationDir.exists()) {
      if (!destinationDir.mkdirs()) {
        throw new IOException("Unable to create local dir: " + destinationDir);
      }
    }
    URL downloadURL = new URL(apacheMirror + String
        .format(APACHE_DOWNLOAD_MIRROR_SUFFIX_FORMAT, version, version));
    log.info("Downloading tarball from: <{}> to <{}>", downloadURL,
        destinationFile.getAbsolutePath());
    FileUtils.copyURLToFile(downloadURL, destinationFile, 10000, 60000);
    log.info("Completed downloading of Hadoop tarball");
    return destinationFile;
  }

  /**
   * Get the URI that can be used to access the launched NameNode for HDFS RPCs.
   *
   * @param nameNodeProperties The set of properties representing the
   *                           information about the launched NameNode.
   * @return The HDFS URI.
   */
  static URI getNameNodeHdfsUri(Properties nameNodeProperties) {
    return URI.create(String.format("hdfs://%s:%s/",
        nameNodeProperties.getProperty(DynoConstants.NN_HOSTNAME),
        nameNodeProperties.getProperty(DynoConstants.NN_RPC_PORT)));
  }

  /**
   * Get the URI that can be used to access the launched NameNode for HDFS
   * Service RPCs (i.e. from DataNodes).
   *
   * @param nameNodeProperties The set of properties representing the
   *                           information about the launched NameNode.
   * @return The service RPC URI.
   */
  static URI getNameNodeServiceRpcAddr(Properties nameNodeProperties) {
    return URI.create(String.format("hdfs://%s:%s/",
        nameNodeProperties.getProperty(DynoConstants.NN_HOSTNAME),
        nameNodeProperties.getProperty(DynoConstants.NN_SERVICERPC_PORT)));
  }

  /**
   * Get the URI that can be used to access the launched NameNode's web UI, e.g.
   * for JMX calls.
   *
   * @param nameNodeProperties The set of properties representing the
   *                           information about the launched NameNode.
   * @return The URI to the web UI.
   */
  static URI getNameNodeWebUri(Properties nameNodeProperties) {
    return URI.create(String.format("http://%s:%s/",
        nameNodeProperties.getProperty(DynoConstants.NN_HOSTNAME),
        nameNodeProperties.getProperty(DynoConstants.NN_HTTP_PORT)));
  }

  /**
   * Get the URI that can be used to access the tracking interface for the
   * NameNode, i.e. the web UI of the NodeManager hosting the NameNode
   * container.
   *
   * @param nameNodeProperties The set of properties representing the
   *                           information about the launched NameNode.
   * @return The tracking URI.
   */
  static URI getNameNodeTrackingUri(Properties nameNodeProperties)
      throws IOException {
    return URI.create(String.format("http://%s:%s/node/containerlogs/%s/%s/",
        nameNodeProperties.getProperty(DynoConstants.NN_HOSTNAME),
        nameNodeProperties.getProperty(Environment.NM_HTTP_PORT.name()),
        nameNodeProperties.getProperty(Environment.CONTAINER_ID.name()),
        UserGroupInformation.getCurrentUser().getShortUserName()));
  }

  /**
   * Get the set of properties representing information about the launched
   * NameNode. This method will wait for the information to be available until
   * it is interrupted, or {@code shouldExit} returns true. It polls for a file
   * present at {@code nameNodeInfoPath} once a second and uses that file to
   * load the NameNode information.
   *
   * @param shouldExit Should return true iff this should stop waiting.
   * @param conf The configuration.
   * @param nameNodeInfoPath The path at which to expect the NameNode
   *                         information file to be present.
   * @param log Where to log information.
   * @return Absent if this exited prematurely (i.e. due to {@code shouldExit}),
   *         else returns a set of properties representing information about the
   *         launched NameNode.
   */
  static Optional<Properties> waitForAndGetNameNodeProperties(
      Supplier<Boolean> shouldExit, Configuration conf, Path nameNodeInfoPath,
      Logger log) throws IOException, InterruptedException {
    while (!shouldExit.get()) {
      try (FSDataInputStream nnInfoInputStream = nameNodeInfoPath
          .getFileSystem(conf).open(nameNodeInfoPath)) {
        Properties nameNodeProperties = new Properties();
        nameNodeProperties.load(nnInfoInputStream);
        return Optional.of(nameNodeProperties);
      } catch (FileNotFoundException fnfe) {
        log.debug("NameNode host information not yet available");
        Thread.sleep(1000);
      } catch (IOException ioe) {
        log.warn("Unable to fetch NameNode host information; retrying", ioe);
        Thread.sleep(1000);
      }
    }
    return Optional.empty();
  }

  /**
   * Wait for the launched NameNode to finish starting up. Continues until
   * {@code shouldExit} returns true.
   *
   * @param nameNodeProperties The set of properties containing information
   *                           about the NameNode.
   * @param shouldExit Should return true iff this should stop waiting.
   * @param log Where to log information.
   */
  static void waitForNameNodeStartup(Properties nameNodeProperties,
      Supplier<Boolean> shouldExit, Logger log)
      throws IOException, InterruptedException {
    if (shouldExit.get()) {
      return;
    }
    log.info("Waiting for NameNode to finish starting up...");
    waitForNameNodeJMXValue("Startup progress",
        NAMENODE_STARTUP_PROGRESS_JMX_QUERY, "PercentComplete", 1.0, 0.01,
        false, nameNodeProperties, shouldExit, log);
    log.info("NameNode has started!");
  }

  /**
   * Wait for the launched NameNode to be ready, i.e. to have at least 99% of
   * its DataNodes register, have fewer than 0.01% of its blocks missing, and
   * less than 1% of its blocks under replicated. Continues until the criteria
   * have been met or {@code shouldExit} returns true.
   *
   * @param nameNodeProperties The set of properties containing information
   *                           about the NameNode.
   * @param numTotalDataNodes Total expected number of DataNodes to register.
   * @param shouldExit Should return true iff this should stop waiting.
   * @param log Where to log information.
   */
  static void waitForNameNodeReadiness(final Properties nameNodeProperties,
      int numTotalDataNodes, boolean triggerBlockReports,
      Supplier<Boolean> shouldExit, final Configuration conf, final Logger log)
      throws IOException, InterruptedException {
    if (shouldExit.get()) {
      return;
    }
    int minDataNodes = (int) (conf.getFloat(DATANODE_LIVE_MIN_FRACTION_KEY,
        DATANODE_LIVE_MIN_FRACTION_DEFAULT) * numTotalDataNodes);
    log.info(String.format(
        "Waiting for %d DataNodes to register with the NameNode...",
        minDataNodes));
    waitForNameNodeJMXValue("Number of live DataNodes",
        FSNAMESYSTEM_STATE_JMX_QUERY, JMX_LIVE_NODE_COUNT, minDataNodes,
        numTotalDataNodes * 0.001, false, nameNodeProperties, shouldExit, log);
    final int totalBlocks = Integer.parseInt(fetchNameNodeJMXValue(
        nameNodeProperties, FSNAMESYSTEM_STATE_JMX_QUERY, JMX_BLOCKS_TOTAL));
    final AtomicBoolean doneWaiting = new AtomicBoolean(false);
    if (triggerBlockReports) {
      // This will be significantly lower than the actual expected number of
      // blocks because it does not
      // take into account replication factor. However the block reports are
      // pretty binary; either a full
      // report has been received or it hasn't. Thus we don't mind the large
      // underestimate here.
      final int blockThreshold = totalBlocks / numTotalDataNodes * 2;
      // The Configuration object here is based on the host cluster, which may
      // have security enabled; we need to disable it to talk to the Dyno NN
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "simple");
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
          "false");
      final DistributedFileSystem dfs = (DistributedFileSystem) FileSystem
          .get(getNameNodeHdfsUri(nameNodeProperties), conf);
      log.info("Launching thread to trigger block reports for Datanodes with <"
          + blockThreshold + " blocks reported");
      Thread blockReportThread = new Thread(() -> {
        // Here we count both Missing and UnderReplicated within under
        // replicated
        long lastUnderRepBlocks = Long.MAX_VALUE;
        try {
          while (true) { // this will eventually exit via an interrupt
            try {
              Thread.sleep(TimeUnit.MINUTES.toMillis(1));
              long underRepBlocks = Long
                  .parseLong(fetchNameNodeJMXValue(nameNodeProperties,
                      FSNAMESYSTEM_JMX_QUERY, JMX_MISSING_BLOCKS))
                  + Long.parseLong(fetchNameNodeJMXValue(nameNodeProperties,
                      FSNAMESYSTEM_STATE_JMX_QUERY,
                      JMX_UNDER_REPLICATED_BLOCKS));
              long blockDecrease = lastUnderRepBlocks - underRepBlocks;
              lastUnderRepBlocks = underRepBlocks;
              if (blockDecrease < 0
                  || blockDecrease > (totalBlocks * 0.001)) {
                continue;
              }

              String liveNodeListString = fetchNameNodeJMXValue(
                  nameNodeProperties, NAMENODE_INFO_JMX_QUERY,
                  JMX_LIVE_NODES_LIST);
              Set<String> datanodesToReport = parseStaleDataNodeList(
                  liveNodeListString, blockThreshold, log);
              if (datanodesToReport.isEmpty() && doneWaiting.get()) {
                log.info("BlockReportThread exiting; all DataNodes have "
                    + "reported blocks");
                break;
              }
              log.info("Queueing {} Datanodes for block report: {}",
                      datanodesToReport.size(),
                      Joiner.on(",").join(datanodesToReport));
              DatanodeInfo[] datanodes = dfs.getDataNodeStats();
              int cnt = 0;
              for (DatanodeInfo datanode : datanodes) {
                if (datanodesToReport.contains(datanode.getXferAddr(true))) {
                  Thread.sleep(1); // to throw an interrupt if one is found
                  triggerDataNodeBlockReport(conf, datanode.getIpcAddr(true));
                  cnt++;
                  Thread.sleep(1000);
                }
              }
              if (cnt != datanodesToReport.size()) {
                log.warn("Found {} Datanodes to queue block reports for but "
                        + "was only able to trigger {}",
                    datanodesToReport.size(), cnt);
              }
            } catch (IOException ioe) {
              log.warn("Exception encountered in block report thread", ioe);
            }
          }
        } catch (InterruptedException ie) {
          // Do nothing; just exit
        }
        log.info("Block reporting thread exiting");
      });
      blockReportThread.setDaemon(true);
      blockReportThread
          .setUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
      blockReportThread.start();
    }
    float maxMissingBlocks = totalBlocks * conf.getFloat(
        MISSING_BLOCKS_MAX_FRACTION_KEY, MISSING_BLOCKS_MAX_FRACTION_DEFAULT);
    log.info("Waiting for MissingBlocks to fall below {}...",
        maxMissingBlocks);
    waitForNameNodeJMXValue("Number of missing blocks", FSNAMESYSTEM_JMX_QUERY,
        JMX_MISSING_BLOCKS, maxMissingBlocks, totalBlocks * 0.0001, true,
        nameNodeProperties, shouldExit, log);
    float maxUnderreplicatedBlocks = totalBlocks
        * conf.getFloat(UNDERREPLICATED_BLOCKS_MAX_FRACTION_KEY,
            UNDERREPLICATED_BLOCKS_MAX_FRACTION_DEFAULT);
    log.info("Waiting for UnderReplicatedBlocks to fall below {}...",
        maxUnderreplicatedBlocks);
    waitForNameNodeJMXValue("Number of under replicated blocks",
        FSNAMESYSTEM_STATE_JMX_QUERY, JMX_UNDER_REPLICATED_BLOCKS,
        maxUnderreplicatedBlocks, totalBlocks * 0.001, true, nameNodeProperties,
        shouldExit, log);
    log.info("NameNode is ready for use!");
    doneWaiting.set(true);
  }

  /**
   * Trigger a block report on a given DataNode.
   *
   * @param conf Configuration
   * @param dataNodeTarget The target; should be like {@code <host>:<port>}
   */
  private static void triggerDataNodeBlockReport(Configuration conf,
      String dataNodeTarget) throws IOException {
    InetSocketAddress datanodeAddr = NetUtils.createSocketAddr(dataNodeTarget);

    ClientDatanodeProtocol dnProtocol = DFSUtilClient
        .createClientDatanodeProtocolProxy(datanodeAddr,
            UserGroupInformation.getCurrentUser(), conf,
            NetUtils.getSocketFactory(conf, ClientDatanodeProtocol.class));

    dnProtocol.triggerBlockReport(new BlockReportOptions.Factory().build());
  }

  /**
   * Poll the launched NameNode's JMX for a specific value, waiting for it to
   * cross some threshold. Continues until the threshold has been crossed or
   * {@code shouldExit} returns true. Periodically logs the current value.
   *
   * @param valueName The human-readable name of the value which is being
   *                  polled (for printing purposes only).
   * @param jmxBeanQuery The JMX bean query to execute; should return a JMX
   *                     property matching {@code jmxProperty}.
   * @param jmxProperty The name of the JMX property whose value should be
   *                    polled.
   * @param threshold The threshold value to wait for the JMX property to be
   *                  above/below.
   * @param printThreshold The threshold between each log statement; controls
   *                       how frequently the value is printed. For example,
   *                       if this was 10, a statement would be logged every
   *                       time the value has changed by more than 10.
   * @param decreasing True iff the property's value is decreasing and this
   *                   should wait until it is lower than threshold; else the
   *                   value is treated as increasing and will wait until it
   *                   is higher than threshold.
   * @param nameNodeProperties The set of properties containing information
   *                           about the NameNode.
   * @param shouldExit Should return true iff this should stop waiting.
   * @param log Where to log information.
   */
  @SuppressWarnings("checkstyle:parameternumber")
  private static void waitForNameNodeJMXValue(String valueName,
      String jmxBeanQuery, String jmxProperty, double threshold,
      double printThreshold, boolean decreasing, Properties nameNodeProperties,
      Supplier<Boolean> shouldExit, Logger log) throws InterruptedException {
    double lastPrintedValue = decreasing ? Double.MAX_VALUE : Double.MIN_VALUE;
    double value;
    int retryCount = 0;
    long startTime = Time.monotonicNow();
    while (!shouldExit.get()) {
      try {
        value = Double.parseDouble(fetchNameNodeJMXValue(nameNodeProperties,
            jmxBeanQuery, jmxProperty));
        if ((decreasing && value <= threshold)
            || (!decreasing && value >= threshold)) {
          log.info(String.format(
              "%s = %.2f; %s threshold of %.2f; done waiting after %d ms.",
              valueName, value, decreasing ? "below" : "above", threshold,
              Time.monotonicNow() - startTime));
          break;
        } else if (Math.abs(value - lastPrintedValue) >= printThreshold) {
          log.info(String.format("%s: %.2f", valueName, value));
          lastPrintedValue = value;
        }
      } catch (IOException ioe) {
        if (++retryCount % 20 == 0) {
          log.warn("Unable to fetch {}; retried {} times / waited {} ms",
              valueName, retryCount, Time.monotonicNow() - startTime, ioe);
        }
      }
      Thread.sleep(3000);
    }
  }

  static Set<String> parseStaleDataNodeList(String liveNodeJsonString,
      final int blockThreshold, final Logger log) throws IOException {
    final Set<String> dataNodesToReport = new HashSet<>();

    JsonFactory fac = new JsonFactory();
    JsonParser parser = fac.createJsonParser(IOUtils
        .toInputStream(liveNodeJsonString, StandardCharsets.UTF_8.name()));

    int objectDepth = 0;
    String currentNodeAddr = null;
    for (JsonToken tok = parser.nextToken(); tok != null; tok = parser
        .nextToken()) {
      if (tok == JsonToken.START_OBJECT) {
        objectDepth++;
      } else if (tok == JsonToken.END_OBJECT) {
        objectDepth--;
      } else if (tok == JsonToken.FIELD_NAME) {
        if (objectDepth == 1) {
          // This is where the Datanode identifiers are stored
          currentNodeAddr = parser.getCurrentName();
        } else if (objectDepth == 2) {
          if (parser.getCurrentName().equals("numBlocks")) {
            JsonToken valueToken = parser.nextToken();
            if (valueToken != JsonToken.VALUE_NUMBER_INT
                || currentNodeAddr == null) {
              throw new IOException(String.format("Malformed LiveNodes JSON; "
                      + "got token = %s; currentNodeAddr = %s: %s",
                  valueToken, currentNodeAddr, liveNodeJsonString));
            }
            int numBlocks = parser.getIntValue();
            if (numBlocks < blockThreshold) {
              log.debug(String.format(
                  "Queueing Datanode <%s> for block report; numBlocks = %d",
                  currentNodeAddr, numBlocks));
              dataNodesToReport.add(currentNodeAddr);
            } else {
              log.debug(String.format(
                  "Not queueing Datanode <%s> for block report; numBlocks = %d",
                  currentNodeAddr, numBlocks));
            }
          }
        }
      }
    }
    return dataNodesToReport;
  }

  /**
   * Fetch a value from the launched NameNode's JMX.
   *
   * @param nameNodeProperties The set of properties containing information
   *                           about the NameNode.
   * @param jmxBeanQuery The JMX bean query to execute; should return a
   *                     JMX property matching {@code jmxProperty}.
   * @param property The name of the JMX property whose value should be polled.
   * @return The value associated with the property.
   */
  static String fetchNameNodeJMXValue(Properties nameNodeProperties,
      String jmxBeanQuery, String property) throws IOException {
    URI nnWebUri = getNameNodeWebUri(nameNodeProperties);
    URL queryURL;
    try {
      queryURL = new URL(nnWebUri.getScheme(), nnWebUri.getHost(),
          nnWebUri.getPort(), "/jmx?qry=" + jmxBeanQuery);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Invalid JMX query: \"" + jmxBeanQuery
          + "\" against " + "NameNode URI: " + nnWebUri);
    }
    HttpURLConnection conn = (HttpURLConnection) queryURL.openConnection();
    if (conn.getResponseCode() != 200) {
      throw new IOException(
          "Unable to retrieve JMX: " + conn.getResponseMessage());
    }
    InputStream in = conn.getInputStream();
    JsonFactory fac = new JsonFactory();
    JsonParser parser = fac.createJsonParser(in);
    if (parser.nextToken() != JsonToken.START_OBJECT
        || parser.nextToken() != JsonToken.FIELD_NAME
        || !parser.getCurrentName().equals("beans")
        || parser.nextToken() != JsonToken.START_ARRAY
        || parser.nextToken() != JsonToken.START_OBJECT) {
      throw new IOException(
          "Unexpected format of JMX JSON response for: " + jmxBeanQuery);
    }
    int objectDepth = 1;
    String ret = null;
    while (objectDepth > 0) {
      JsonToken tok = parser.nextToken();
      if (tok == JsonToken.START_OBJECT) {
        objectDepth++;
      } else if (tok == JsonToken.END_OBJECT) {
        objectDepth--;
      } else if (tok == JsonToken.FIELD_NAME) {
        if (parser.getCurrentName().equals(property)) {
          parser.nextToken();
          ret = parser.getText();
          break;
        }
      }
    }
    parser.close();
    in.close();
    conn.disconnect();
    if (ret == null) {
      throw new IOException(
          "Property " + property + " not found within " + jmxBeanQuery);
    } else {
      return ret;
    }
  }

}
