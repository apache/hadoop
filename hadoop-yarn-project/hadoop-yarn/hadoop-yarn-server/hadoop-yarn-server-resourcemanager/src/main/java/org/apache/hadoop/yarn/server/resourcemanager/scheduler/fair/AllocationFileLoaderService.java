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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import com.google.common.annotations.VisibleForTesting;

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

  private final Clock clock;

  private long lastSuccessfulReload; // Last time we successfully reloaded queues
  private boolean lastReloadAttemptFailed = false;
  
  // Path to XML file containing allocations. 
  private File allocFile;
  
  private Listener reloadListener;
  
  @VisibleForTesting
  long reloadIntervalMs = ALLOC_RELOAD_INTERVAL_MS;
  
  private Thread reloadThread;
  private volatile boolean running = true;

  public AllocationFileLoaderService() {
    this(new SystemClock());
  }
  
  public AllocationFileLoaderService(Clock clock) {
    super(AllocationFileLoaderService.class.getName());
    this.clock = clock;
    
  }
  
  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.allocFile = getAllocationFile(conf);
    if (allocFile != null) {
      reloadThread = new Thread() {
        @Override
        public void run() {
          while (running) {
            long time = clock.getTime();
            long lastModified = allocFile.lastModified();
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
                    + allocFile.exists());
              }
              lastReloadAttemptFailed = true;
            }
            try {
              Thread.sleep(reloadIntervalMs);
            } catch (InterruptedException ex) {
              LOG.info(
                  "Interrupted while waiting to reload alloc configuration");
            }
          }
        }
      };
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
  public File getAllocationFile(Configuration conf) {
    String allocFilePath = conf.get(FairSchedulerConfiguration.ALLOCATION_FILE,
        FairSchedulerConfiguration.DEFAULT_ALLOCATION_FILE);
    File allocFile = new File(allocFilePath);
    if (!allocFile.isAbsolute()) {
      URL url = Thread.currentThread().getContextClassLoader()
          .getResource(allocFilePath);
      if (url == null) {
        LOG.warn(allocFilePath + " not found on the classpath.");
        allocFile = null;
      } else if (!url.getProtocol().equalsIgnoreCase("file")) {
        throw new RuntimeException("Allocation file " + url
            + " found on the classpath is not on the local filesystem.");
      } else {
        allocFile = new File(url.getPath());
      }
    }
    return allocFile;
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
  public synchronized void reloadAllocations() throws IOException,
      ParserConfigurationException, SAXException, AllocationConfigurationException {
    if (allocFile == null) {
      return;
    }
    LOG.info("Loading allocation file " + allocFile);
    // Create some temporary hashmaps to hold the new allocs, and we only save
    // them in our fields if we have parsed the entire allocs file successfully.
    Map<String, Resource> minQueueResources = new HashMap<String, Resource>();
    Map<String, Resource> maxQueueResources = new HashMap<String, Resource>();
    Map<String, Integer> queueMaxApps = new HashMap<String, Integer>();
    Map<String, Integer> userMaxApps = new HashMap<String, Integer>();
    Map<String, Float> queueMaxAMShares = new HashMap<String, Float>();
    Map<String, ResourceWeights> queueWeights = new HashMap<String, ResourceWeights>();
    Map<String, SchedulingPolicy> queuePolicies = new HashMap<String, SchedulingPolicy>();
    Map<String, Long> minSharePreemptionTimeouts = new HashMap<String, Long>();
    Map<String, Long> fairSharePreemptionTimeouts = new HashMap<String, Long>();
    Map<String, Float> fairSharePreemptionThresholds =
        new HashMap<String, Float>();
    Map<String, Map<QueueACL, AccessControlList>> queueAcls =
        new HashMap<String, Map<QueueACL, AccessControlList>>();
    Set<String> reservableQueues = new HashSet<String>();
    int userMaxAppsDefault = Integer.MAX_VALUE;
    int queueMaxAppsDefault = Integer.MAX_VALUE;
    float queueMaxAMShareDefault = 0.5f;
    long defaultFairSharePreemptionTimeout = Long.MAX_VALUE;
    long defaultMinSharePreemptionTimeout = Long.MAX_VALUE;
    float defaultFairSharePreemptionThreshold = 0.5f;
    SchedulingPolicy defaultSchedPolicy = SchedulingPolicy.DEFAULT_POLICY;

    // Reservation global configuration knobs
    String planner = null;
    String reservationAgent = null;
    String reservationAdmissionPolicy = null;

    QueuePlacementPolicy newPlacementPolicy = null;

    // Remember all queue names so we can display them on web UI, etc.
    // configuredQueues is segregated based on whether it is a leaf queue
    // or a parent queue. This information is used for creating queues
    // and also for making queue placement decisions(QueuePlacementRule.java).
    Map<FSQueueType, Set<String>> configuredQueues =
        new HashMap<FSQueueType, Set<String>>();
    for (FSQueueType queueType : FSQueueType.values()) {
      configuredQueues.put(queueType, new HashSet<String>());
    }

    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(allocFile);
    Element root = doc.getDocumentElement();
    if (!"allocations".equals(root.getTagName()))
      throw new AllocationConfigurationException("Bad fair scheduler config " +
          "file: top-level element not <allocations>");
    NodeList elements = root.getChildNodes();
    List<Element> queueElements = new ArrayList<Element>();
    Element placementPolicyElement = null;
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node instanceof Element) {
        Element element = (Element)node;
        if ("queue".equals(element.getTagName()) ||
          "pool".equals(element.getTagName())) {
          queueElements.add(element);
        } else if ("user".equals(element.getTagName())) {
          String userName = element.getAttribute("name");
          NodeList fields = element.getChildNodes();
          for (int j = 0; j < fields.getLength(); j++) {
            Node fieldNode = fields.item(j);
            if (!(fieldNode instanceof Element))
              continue;
            Element field = (Element) fieldNode;
            if ("maxRunningApps".equals(field.getTagName())) {
              String text = ((Text)field.getFirstChild()).getData().trim();
              int val = Integer.parseInt(text);
              userMaxApps.put(userName, val);
            }
          }
        } else if ("userMaxAppsDefault".equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          int val = Integer.parseInt(text);
          userMaxAppsDefault = val;
        } else if ("defaultFairSharePreemptionTimeout"
            .equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          long val = Long.parseLong(text) * 1000L;
          defaultFairSharePreemptionTimeout = val;
        } else if ("fairSharePreemptionTimeout".equals(element.getTagName())) {
          if (defaultFairSharePreemptionTimeout == Long.MAX_VALUE) {
            String text = ((Text)element.getFirstChild()).getData().trim();
            long val = Long.parseLong(text) * 1000L;
            defaultFairSharePreemptionTimeout = val;
          }
        } else if ("defaultMinSharePreemptionTimeout"
            .equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          long val = Long.parseLong(text) * 1000L;
          defaultMinSharePreemptionTimeout = val;
        } else if ("defaultFairSharePreemptionThreshold"
            .equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          float val = Float.parseFloat(text);
          val = Math.max(Math.min(val, 1.0f), 0.0f);
          defaultFairSharePreemptionThreshold = val;
        } else if ("queueMaxAppsDefault".equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          int val = Integer.parseInt(text);
          queueMaxAppsDefault = val;
        } else if ("queueMaxAMShareDefault".equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          float val = Float.parseFloat(text);
          val = Math.min(val, 1.0f);
          queueMaxAMShareDefault = val;
        } else if ("defaultQueueSchedulingPolicy".equals(element.getTagName())
            || "defaultQueueSchedulingMode".equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          defaultSchedPolicy = SchedulingPolicy.parse(text);
        } else if ("queuePlacementPolicy".equals(element.getTagName())) {
          placementPolicyElement = element;
        } else if ("reservation-planner".equals(element.getTagName())) {
          String text = ((Text) element.getFirstChild()).getData().trim();
          planner = text;
        } else if ("reservation-agent".equals(element.getTagName())) {
          String text = ((Text) element.getFirstChild()).getData().trim();
          reservationAgent = text;
        } else if ("reservation-policy".equals(element.getTagName())) {
          String text = ((Text) element.getFirstChild()).getData().trim();
          reservationAdmissionPolicy = text;
        } else {
          LOG.warn("Bad element in allocations file: " + element.getTagName());
        }
      }
    }

    // Load queue elements.  A root queue can either be included or omitted.  If
    // it's included, all other queues must be inside it.
    for (Element element : queueElements) {
      String parent = "root";
      if (element.getAttribute("name").equalsIgnoreCase("root")) {
        if (queueElements.size() > 1) {
          throw new AllocationConfigurationException("If configuring root queue,"
              + " no other queues can be placed alongside it.");
        }
        parent = null;
      }
      loadQueue(parent, element, minQueueResources, maxQueueResources,
          queueMaxApps, userMaxApps, queueMaxAMShares, queueWeights,
          queuePolicies, minSharePreemptionTimeouts, fairSharePreemptionTimeouts,
          fairSharePreemptionThresholds, queueAcls, configuredQueues,
          reservableQueues);
    }

    // Load placement policy and pass it configured queues
    Configuration conf = getConfig();
    if (placementPolicyElement != null) {
      newPlacementPolicy = QueuePlacementPolicy.fromXml(placementPolicyElement,
          configuredQueues, conf);
    } else {
      newPlacementPolicy = QueuePlacementPolicy.fromConfiguration(conf,
          configuredQueues);
    }

    // Set the min/fair share preemption timeout for the root queue
    if (!minSharePreemptionTimeouts.containsKey(QueueManager.ROOT_QUEUE)){
      minSharePreemptionTimeouts.put(QueueManager.ROOT_QUEUE,
          defaultMinSharePreemptionTimeout);
    }
    if (!fairSharePreemptionTimeouts.containsKey(QueueManager.ROOT_QUEUE)) {
      fairSharePreemptionTimeouts.put(QueueManager.ROOT_QUEUE,
          defaultFairSharePreemptionTimeout);
    }

    // Set the fair share preemption threshold for the root queue
    if (!fairSharePreemptionThresholds.containsKey(QueueManager.ROOT_QUEUE)) {
      fairSharePreemptionThresholds.put(QueueManager.ROOT_QUEUE,
          defaultFairSharePreemptionThreshold);
    }

    ReservationQueueConfiguration globalReservationQueueConfig = new
        ReservationQueueConfiguration();
    if (planner != null) {
      globalReservationQueueConfig.setPlanner(planner);
    }
    if (reservationAdmissionPolicy != null) {
      globalReservationQueueConfig.setReservationAdmissionPolicy
          (reservationAdmissionPolicy);
    }
    if (reservationAgent != null) {
      globalReservationQueueConfig.setReservationAgent(reservationAgent);
    }

    AllocationConfiguration info = new AllocationConfiguration(minQueueResources,
        maxQueueResources, queueMaxApps, userMaxApps, queueWeights,
        queueMaxAMShares, userMaxAppsDefault, queueMaxAppsDefault,
        queueMaxAMShareDefault, queuePolicies, defaultSchedPolicy,
        minSharePreemptionTimeouts, fairSharePreemptionTimeouts,
        fairSharePreemptionThresholds, queueAcls,
        newPlacementPolicy, configuredQueues, globalReservationQueueConfig,
        reservableQueues);
    
    lastSuccessfulReload = clock.getTime();
    lastReloadAttemptFailed = false;

    reloadListener.onReload(info);
  }
  
  /**
   * Loads a queue from a queue element in the configuration file
   */
  private void loadQueue(String parentName, Element element,
      Map<String, Resource> minQueueResources,
      Map<String, Resource> maxQueueResources, Map<String, Integer> queueMaxApps,
      Map<String, Integer> userMaxApps, Map<String, Float> queueMaxAMShares,
      Map<String, ResourceWeights> queueWeights,
      Map<String, SchedulingPolicy> queuePolicies,
      Map<String, Long> minSharePreemptionTimeouts,
      Map<String, Long> fairSharePreemptionTimeouts,
      Map<String, Float> fairSharePreemptionThresholds,
      Map<String, Map<QueueACL, AccessControlList>> queueAcls,
      Map<FSQueueType, Set<String>> configuredQueues,
      Set<String> reservableQueues)
      throws AllocationConfigurationException {
    String queueName = element.getAttribute("name");

    if (queueName.contains(".")) {
      throw new AllocationConfigurationException("Bad fair scheduler config "
          + "file: queue name (" + queueName + ") shouldn't contain period.");
    }

    if (parentName != null) {
      queueName = parentName + "." + queueName;
    }
    Map<QueueACL, AccessControlList> acls =
        new HashMap<QueueACL, AccessControlList>();
    NodeList fields = element.getChildNodes();
    boolean isLeaf = true;

    for (int j = 0; j < fields.getLength(); j++) {
      Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element))
        continue;
      Element field = (Element) fieldNode;
      if ("minResources".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        Resource val = FairSchedulerConfiguration.parseResourceConfigValue(text);
        minQueueResources.put(queueName, val);
      } else if ("maxResources".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        Resource val = FairSchedulerConfiguration.parseResourceConfigValue(text);
        maxQueueResources.put(queueName, val);
      } else if ("maxRunningApps".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        queueMaxApps.put(queueName, val);
      } else if ("maxAMShare".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        float val = Float.parseFloat(text);
        val = Math.min(val, 1.0f);
        queueMaxAMShares.put(queueName, val);
      } else if ("weight".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        double val = Double.parseDouble(text);
        queueWeights.put(queueName, new ResourceWeights((float)val));
      } else if ("minSharePreemptionTimeout".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        long val = Long.parseLong(text) * 1000L;
        minSharePreemptionTimeouts.put(queueName, val);
      } else if ("fairSharePreemptionTimeout".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        long val = Long.parseLong(text) * 1000L;
        fairSharePreemptionTimeouts.put(queueName, val);
      } else if ("fairSharePreemptionThreshold".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        float val = Float.parseFloat(text);
        val = Math.max(Math.min(val, 1.0f), 0.0f);
        fairSharePreemptionThresholds.put(queueName, val);
      } else if ("schedulingPolicy".equals(field.getTagName())
          || "schedulingMode".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        SchedulingPolicy policy = SchedulingPolicy.parse(text);
        queuePolicies.put(queueName, policy);
      } else if ("aclSubmitApps".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData();
        acls.put(QueueACL.SUBMIT_APPLICATIONS, new AccessControlList(text));
      } else if ("aclAdministerApps".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData();
        acls.put(QueueACL.ADMINISTER_QUEUE, new AccessControlList(text));
      } else if ("reservation".equals(field.getTagName())) {
        isLeaf = false;
        reservableQueues.add(queueName);
        configuredQueues.get(FSQueueType.PARENT).add(queueName);
      } else if ("queue".endsWith(field.getTagName()) || 
          "pool".equals(field.getTagName())) {
        loadQueue(queueName, field, minQueueResources, maxQueueResources,
            queueMaxApps, userMaxApps, queueMaxAMShares, queueWeights,
            queuePolicies, minSharePreemptionTimeouts,
            fairSharePreemptionTimeouts, fairSharePreemptionThresholds,
            queueAcls, configuredQueues, reservableQueues);
        isLeaf = false;
      }
    }
    if (isLeaf) {
      // if a leaf in the alloc file is marked as type='parent'
      // then store it under 'parent'
      if ("parent".equals(element.getAttribute("type"))) {
        configuredQueues.get(FSQueueType.PARENT).add(queueName);
      } else {
        configuredQueues.get(FSQueueType.LEAF).add(queueName);
      }
    } else {
      if ("parent".equals(element.getAttribute("type"))) {
        throw new AllocationConfigurationException("Both <reservation> and " +
            "type=\"parent\" found for queue " + queueName + " which is " +
            "unsupported");
      }
      configuredQueues.get(FSQueueType.PARENT).add(queueName);
    }
    queueAcls.put(queueName, acls);
    if (maxQueueResources.containsKey(queueName) &&
        minQueueResources.containsKey(queueName)
        && !Resources.fitsIn(minQueueResources.get(queueName),
            maxQueueResources.get(queueName))) {
      LOG.warn(
          String.format(
              "Queue %s has max resources %s less than min resources %s",
          queueName, maxQueueResources.get(queueName),
              minQueueResources.get(queueName)));
    }
  }
  
  public interface Listener {
    public void onReload(AllocationConfiguration info);
  }
}
