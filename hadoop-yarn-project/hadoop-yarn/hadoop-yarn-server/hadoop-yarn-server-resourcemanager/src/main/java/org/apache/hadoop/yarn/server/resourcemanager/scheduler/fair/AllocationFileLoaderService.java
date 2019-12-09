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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileQueueParser;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.QueueProperties;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileQueueParser.EVERYBODY_ACL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocation.AllocationFileQueueParser.ROOT;

@Public
@Unstable
public class AllocationFileLoaderService extends AbstractService {

  public static final Log LOG = LogFactory.getLog(
      AllocationFileLoaderService.class.getName());

  /** Time to wait between checks of the allocation file */
  public static final long ALLOC_RELOAD_INTERVAL_MS = 10 * 1000;

  /**
   * Time to wait after the allocation has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long ALLOC_RELOAD_WAIT_MS = 5 * 1000;

  public static final long THREAD_JOIN_TIMEOUT_MS = 1000;

  //Permitted allocation file filesystems (case insensitive)
  private static final String SUPPORTED_FS_REGEX =
      "(?i)(hdfs)|(file)|(s3a)|(viewfs)";

  private final Clock clock;

  // Last time we successfully reloaded queues
  private volatile long lastSuccessfulReload;
  private volatile boolean lastReloadAttemptFailed = false;

  // Path to XML file containing allocations.
  private Path allocFile;
  private FileSystem fs;

  private Listener reloadListener;

  @VisibleForTesting
  long reloadIntervalMs = ALLOC_RELOAD_INTERVAL_MS;

  private Thread reloadThread;
  private volatile boolean running = true;

  public AllocationFileLoaderService() {
    this(SystemClock.getInstance());
  }

  private List<Permission> defaultPermissions;

  public AllocationFileLoaderService(Clock clock) {
    super(AllocationFileLoaderService.class.getName());
    this.clock = clock;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.allocFile = getAllocationFile(conf);
    if (this.allocFile != null) {
      this.fs = allocFile.getFileSystem(conf);
      reloadThread = new Thread(() -> {
        while (running) {
          try {
            synchronized (this) {
              reloadListener.onCheck();
            }
            long time = clock.getTime();
            long lastModified =
                fs.getFileStatus(allocFile).getModificationTime();
            if (lastModified > lastSuccessfulReload &&
                time > lastModified + ALLOC_RELOAD_WAIT_MS) {
              try {
                reloadAllocations();
              } catch (Exception ex) {
                if (!lastReloadAttemptFailed) {
                  LOG.error("Failed to reload fair scheduler config file - " +
                      "will use existing allocations.", ex);
                }
                lastReloadAttemptFailed = true;
              }
            } else if (lastModified == 0l) {
              if (!lastReloadAttemptFailed) {
                LOG.warn("Failed to reload fair scheduler config file because" +
                    " last modified returned 0. File exists: "
                    + fs.exists(allocFile));
              }
              lastReloadAttemptFailed = true;
            }
          } catch (IOException e) {
            LOG.error("Exception while loading allocation file: " + e);
          }
          try {
            Thread.sleep(reloadIntervalMs);
          } catch (InterruptedException ex) {
            LOG.info(
                "Interrupted while waiting to reload alloc configuration");
          }
        }
      });
      reloadThread.setName("AllocationFileReloader");
      reloadThread.setDaemon(true);
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    if (reloadThread != null) {
      reloadThread.start();
    }
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    running = false;
    if (reloadThread != null) {
      reloadThread.interrupt();
      try {
        reloadThread.join(THREAD_JOIN_TIMEOUT_MS);
      } catch (InterruptedException e) {
        LOG.warn("reloadThread fails to join.");
      }
    }
    super.serviceStop();
  }

  /**
   * Path to XML file containing allocations. If the
   * path is relative, it is searched for in the
   * classpath, but loaded like a regular File.
   */
  @VisibleForTesting
  Path getAllocationFile(Configuration conf)
      throws UnsupportedFileSystemException {
    String allocFilePath = conf.get(FairSchedulerConfiguration.ALLOCATION_FILE,
        FairSchedulerConfiguration.DEFAULT_ALLOCATION_FILE);
    Path allocPath = new Path(allocFilePath);
    String allocPathScheme = allocPath.toUri().getScheme();
    if(allocPathScheme != null && !allocPathScheme.matches(SUPPORTED_FS_REGEX)){
      throw new UnsupportedFileSystemException("Allocation file "
          + allocFilePath + " uses an unsupported filesystem");
    } else if (!allocPath.isAbsolute()) {
      URL url = Thread.currentThread().getContextClassLoader()
          .getResource(allocFilePath);
      if (url == null) {
        LOG.warn(allocFilePath + " not found on the classpath.");
        allocPath = null;
      } else if (!url.getProtocol().equalsIgnoreCase("file")) {
        throw new RuntimeException("Allocation file " + url
            + " found on the classpath is not on the local filesystem.");
      } else {
        allocPath = new Path(url.getProtocol(), null, url.getPath());
      }
    } else if (allocPath.isAbsoluteAndSchemeAuthorityNull()){
      allocPath = new Path("file", null, allocFilePath);
    }
    return allocPath;
  }

  public synchronized void setReloadListener(Listener reloadListener) {
    this.reloadListener = reloadListener;
  }

  /**
   * Updates the allocation list from the allocation config file. This file is
   * expected to be in the XML format specified in the design doc.
   *
   * @throws IOException if the config file cannot be read.
   * @throws AllocationConfigurationException if allocations are invalid.
   * @throws ParserConfigurationException if XML parser is misconfigured.
   * @throws SAXException if config file is malformed.
   */
  public synchronized void reloadAllocations()
      throws IOException, ParserConfigurationException, SAXException,
      AllocationConfigurationException {
    if (allocFile == null) {
      reloadListener.onReload(null);
      return;
    }
    LOG.info("Loading allocation file " + allocFile);

    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory =
        DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(fs.open(allocFile));
    Element root = doc.getDocumentElement();
    if (!"allocations".equals(root.getTagName())) {
      throw new AllocationConfigurationException("Bad fair scheduler config "
          + "file: top-level element not <allocations>");
    }
    NodeList elements = root.getChildNodes();

    AllocationFileParser allocationFileParser =
        new AllocationFileParser(elements);
    allocationFileParser.parse();

    AllocationFileQueueParser queueParser =
        new AllocationFileQueueParser(allocationFileParser.getQueueElements());
    QueueProperties queueProperties = queueParser.parse();

    // Load placement policy and pass it configured queues
    Configuration conf = getConfig();
    QueuePlacementPolicy newPlacementPolicy =
        getQueuePlacementPolicy(allocationFileParser, queueProperties, conf);
    setupRootQueueProperties(allocationFileParser, queueProperties);

    ReservationQueueConfiguration globalReservationQueueConfig =
        createReservationQueueConfig(allocationFileParser);

    AllocationConfiguration info = new AllocationConfiguration(queueProperties,
        allocationFileParser, newPlacementPolicy, globalReservationQueueConfig);

    lastSuccessfulReload = clock.getTime();
    lastReloadAttemptFailed = false;

    reloadListener.onReload(info);
  }

  private QueuePlacementPolicy getQueuePlacementPolicy(
      AllocationFileParser allocationFileParser,
      QueueProperties queueProperties, Configuration conf)
      throws AllocationConfigurationException {
    if (allocationFileParser.getQueuePlacementPolicy().isPresent()) {
      return QueuePlacementPolicy.fromXml(
          allocationFileParser.getQueuePlacementPolicy().get(),
          queueProperties.getConfiguredQueues(), conf);
    } else {
      return QueuePlacementPolicy.fromConfiguration(conf,
          queueProperties.getConfiguredQueues());
    }
  }

  private void setupRootQueueProperties(
      AllocationFileParser allocationFileParser,
      QueueProperties queueProperties) {
    // Set the min/fair share preemption timeout for the root queue
    if (!queueProperties.getMinSharePreemptionTimeouts()
        .containsKey(QueueManager.ROOT_QUEUE)) {
      queueProperties.getMinSharePreemptionTimeouts().put(
          QueueManager.ROOT_QUEUE,
          allocationFileParser.getDefaultMinSharePreemptionTimeout());
    }
    if (!queueProperties.getFairSharePreemptionTimeouts()
        .containsKey(QueueManager.ROOT_QUEUE)) {
      queueProperties.getFairSharePreemptionTimeouts().put(
          QueueManager.ROOT_QUEUE,
          allocationFileParser.getDefaultFairSharePreemptionTimeout());
    }

    // Set the fair share preemption threshold for the root queue
    if (!queueProperties.getFairSharePreemptionThresholds()
        .containsKey(QueueManager.ROOT_QUEUE)) {
      queueProperties.getFairSharePreemptionThresholds().put(
          QueueManager.ROOT_QUEUE,
          allocationFileParser.getDefaultFairSharePreemptionThreshold());
    }
  }

  private ReservationQueueConfiguration createReservationQueueConfig(
      AllocationFileParser allocationFileParser) {
    ReservationQueueConfiguration globalReservationQueueConfig =
        new ReservationQueueConfiguration();
    if (allocationFileParser.getReservationPlanner().isPresent()) {
      globalReservationQueueConfig
          .setPlanner(allocationFileParser.getReservationPlanner().get());
    }
    if (allocationFileParser.getReservationAdmissionPolicy().isPresent()) {
      globalReservationQueueConfig.setReservationAdmissionPolicy(
          allocationFileParser.getReservationAdmissionPolicy().get());
    }
    if (allocationFileParser.getReservationAgent().isPresent()) {
      globalReservationQueueConfig.setReservationAgent(
          allocationFileParser.getReservationAgent().get());
    }
    return globalReservationQueueConfig;
  }

  /**
   * Returns the list of default permissions.
   * The default permission for the root queue is everybody ("*")
   * and the default permission for all other queues is nobody ("").
   * The default permission list would be loaded before the permissions
   * from allocation file.
   * @return default permission list
   */
  protected List<Permission> getDefaultPermissions() {
    if (defaultPermissions == null) {
      defaultPermissions = new ArrayList<>();
      Map<AccessType, AccessControlList> acls =
          new HashMap<>();
      for (QueueACL acl : QueueACL.values()) {
        acls.put(SchedulerUtils.toAccessType(acl), EVERYBODY_ACL);
      }
      defaultPermissions.add(new Permission(
          new PrivilegedEntity(EntityType.QUEUE, ROOT), acls));
    }
    return defaultPermissions;
  }

  public interface Listener {
    void onReload(AllocationConfiguration info) throws IOException;

    default void onCheck() {
    }
  }
}
