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
  public void init(Configuration conf) {
    this.allocFile = getAllocationFile(conf);
    super.init(conf);
  }
  
  @Override
  public void start() {
    if (allocFile == null) {
      return;
    }
    reloadThread = new Thread() {
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
                  " last modified returned 0. File exists: " + allocFile.exists());
            }
            lastReloadAttemptFailed = true;
          }
          try {
            Thread.sleep(reloadIntervalMs);
          } catch (InterruptedException ex) {
            LOG.info("Interrupted while waiting to reload alloc configuration");
          }
        }
      }
    };
    reloadThread.setName("AllocationFileReloader");
    reloadThread.setDaemon(true);
    reloadThread.start();
    super.start();
  }
  
  @Override
  public void stop() {
    running = false;
    reloadThread.interrupt();
    super.stop();
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
    Map<String, ResourceWeights> queueWeights = new HashMap<String, ResourceWeights>();
    Map<String, SchedulingPolicy> queuePolicies = new HashMap<String, SchedulingPolicy>();
    Map<String, Long> minSharePreemptionTimeouts = new HashMap<String, Long>();
    Map<String, Map<QueueACL, AccessControlList>> queueAcls =
        new HashMap<String, Map<QueueACL, AccessControlList>>();
    int userMaxAppsDefault = Integer.MAX_VALUE;
    int queueMaxAppsDefault = Integer.MAX_VALUE;
    long fairSharePreemptionTimeout = Long.MAX_VALUE;
    long defaultMinSharePreemptionTimeout = Long.MAX_VALUE;
    SchedulingPolicy defaultSchedPolicy = SchedulingPolicy.DEFAULT_POLICY;
    
    QueuePlacementPolicy newPlacementPolicy = null;

    // Remember all queue names so we can display them on web UI, etc.
    Set<String> queueNamesInAllocFile = new HashSet<String>();

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
        } else if ("fairSharePreemptionTimeout".equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          long val = Long.parseLong(text) * 1000L;
          fairSharePreemptionTimeout = val;
        } else if ("defaultMinSharePreemptionTimeout".equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          long val = Long.parseLong(text) * 1000L;
          defaultMinSharePreemptionTimeout = val;
        } else if ("queueMaxAppsDefault".equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          int val = Integer.parseInt(text);
          queueMaxAppsDefault = val;
        } else if ("defaultQueueSchedulingPolicy".equals(element.getTagName())
            || "defaultQueueSchedulingMode".equals(element.getTagName())) {
          String text = ((Text)element.getFirstChild()).getData().trim();
          defaultSchedPolicy = SchedulingPolicy.parse(text);
        } else if ("queuePlacementPolicy".equals(element.getTagName())) {
          placementPolicyElement = element;
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
      loadQueue(parent, element, minQueueResources, maxQueueResources, queueMaxApps,
          userMaxApps, queueWeights, queuePolicies, minSharePreemptionTimeouts,
          queueAcls, queueNamesInAllocFile);
    }
    
    // Load placement policy and pass it configured queues
    Configuration conf = getConfig();
    if (placementPolicyElement != null) {
      newPlacementPolicy = QueuePlacementPolicy.fromXml(placementPolicyElement,
          queueNamesInAllocFile, conf);
    } else {
      newPlacementPolicy = QueuePlacementPolicy.fromConfiguration(conf,
          queueNamesInAllocFile);
    }
    
    AllocationConfiguration info = new AllocationConfiguration(minQueueResources, maxQueueResources,
        queueMaxApps, userMaxApps, queueWeights, userMaxAppsDefault,
        queueMaxAppsDefault, queuePolicies, defaultSchedPolicy, minSharePreemptionTimeouts,
        queueAcls, fairSharePreemptionTimeout, defaultMinSharePreemptionTimeout,
        newPlacementPolicy, queueNamesInAllocFile);
    
    lastSuccessfulReload = clock.getTime();
    lastReloadAttemptFailed = false;

    reloadListener.onReload(info);
  }
  
  /**
   * Loads a queue from a queue element in the configuration file
   */
  private void loadQueue(String parentName, Element element, Map<String, Resource> minQueueResources,
      Map<String, Resource> maxQueueResources, Map<String, Integer> queueMaxApps,
      Map<String, Integer> userMaxApps, Map<String, ResourceWeights> queueWeights,
      Map<String, SchedulingPolicy> queuePolicies,
      Map<String, Long> minSharePreemptionTimeouts,
      Map<String, Map<QueueACL, AccessControlList>> queueAcls, Set<String> queueNamesInAllocFile) 
      throws AllocationConfigurationException {
    String queueName = element.getAttribute("name");
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
      } else if ("weight".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        double val = Double.parseDouble(text);
        queueWeights.put(queueName, new ResourceWeights((float)val));
      } else if ("minSharePreemptionTimeout".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        long val = Long.parseLong(text) * 1000L;
        minSharePreemptionTimeouts.put(queueName, val);
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
      } else if ("queue".endsWith(field.getTagName()) || 
          "pool".equals(field.getTagName())) {
        loadQueue(queueName, field, minQueueResources, maxQueueResources,
            queueMaxApps, userMaxApps, queueWeights, queuePolicies,
            minSharePreemptionTimeouts,
            queueAcls, queueNamesInAllocFile);
        isLeaf = false;
      }
    }
    if (isLeaf) {
      queueNamesInAllocFile.add(queueName);
    }
    queueAcls.put(queueName, acls);
    if (maxQueueResources.containsKey(queueName) && minQueueResources.containsKey(queueName)
        && !Resources.fitsIn(minQueueResources.get(queueName),
            maxQueueResources.get(queueName))) {
      LOG.warn(String.format("Queue %s has max resources %s less than min resources %s",
          queueName, maxQueueResources.get(queueName), minQueueResources.get(queueName)));
    }
  }
  
  public interface Listener {
    public void onReload(AllocationConfiguration info);
  }
}
