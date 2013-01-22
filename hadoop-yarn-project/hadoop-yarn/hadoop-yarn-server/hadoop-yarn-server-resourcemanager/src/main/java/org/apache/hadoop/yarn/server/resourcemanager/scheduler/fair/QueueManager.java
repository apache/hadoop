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
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Maintains a list of queues as well as scheduling parameters for each queue,
 * such as guaranteed share allocations, from the fair scheduler config file.
 * 
 */
@Private
@Unstable
public class QueueManager {
  public static final Log LOG = LogFactory.getLog(
    QueueManager.class.getName());

  public static final String ROOT_QUEUE = "root";
  
  /** Time to wait between checks of the allocation file */
  public static final long ALLOC_RELOAD_INTERVAL = 10 * 1000;

  /**
   * Time to wait after the allocation has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long ALLOC_RELOAD_WAIT = 5 * 1000;

  private final FairScheduler scheduler;

  private Object allocFile; // Path to XML file containing allocations. This
                            // is either a URL to specify a classpath resource
                            // (if the fair-scheduler.xml on the classpath is
                            // used) or a String to specify an absolute path (if
                            // mapred.fairscheduler.allocation.file is used).

  private final Collection<FSLeafQueue> leafQueues = 
      new CopyOnWriteArrayList<FSLeafQueue>();
  private final Map<String, FSQueue> queues = new HashMap<String, FSQueue>();
  private FSParentQueue rootQueue;

  private volatile QueueManagerInfo info = new QueueManagerInfo();
  
  private long lastReloadAttempt; // Last time we tried to reload the queues file
  private long lastSuccessfulReload; // Last time we successfully reloaded queues
  private boolean lastReloadAttemptFailed = false;
  
  public QueueManager(FairScheduler scheduler) {
    this.scheduler = scheduler;
  }
  
  public FSParentQueue getRootQueue() {
    return rootQueue;
  }

  public void initialize() throws IOException, SAXException,
      AllocationConfigurationException, ParserConfigurationException {
    FairSchedulerConfiguration conf = scheduler.getConf();
    rootQueue = new FSParentQueue("root", this, scheduler, null);
    queues.put(rootQueue.getName(), rootQueue);
    
    this.allocFile = conf.getAllocationFile();
    if (allocFile == null) {
      // No allocation file specified in jobconf. Use the default allocation
      // file, fair-scheduler.xml, looking for it on the classpath.
      allocFile = new Configuration().getResource("fair-scheduler.xml");
      if (allocFile == null) {
        LOG.error("The fair scheduler allocation file fair-scheduler.xml was "
            + "not found on the classpath, and no other config file is given "
            + "through mapred.fairscheduler.allocation.file.");
      }
    }
    reloadAllocs();
    lastSuccessfulReload = scheduler.getClock().getTime();
    lastReloadAttempt = scheduler.getClock().getTime();
    // Create the default queue
    getLeafQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
  }
  
  /**
   * Get a queue by name, creating it if necessary.  If the queue
   * is not or can not be a leaf queue, i.e. it already exists as a parent queue,
   * or one of the parents in its name is already a leaf queue, null is returned.
   * 
   * The root part of the name is optional, so a queue underneath the root 
   * named "queue1" could be referred to  as just "queue1", and a queue named
   * "queue2" underneath a parent named "parent1" that is underneath the root 
   * could be referred to as just "parent1.queue2".
   */
  public FSLeafQueue getLeafQueue(String name) {
    if (!name.startsWith(ROOT_QUEUE + ".")) {
      name = ROOT_QUEUE + "." + name;
    }
    synchronized (queues) {
      FSQueue queue = queues.get(name);
      if (queue == null) {
        FSLeafQueue leafQueue = createLeafQueue(name);
        if (leafQueue == null) {
          return null;
        }
        leafQueue.setSchedulingMode(info.defaultSchedulingMode);
        queue = leafQueue;
      } else if (queue instanceof FSParentQueue) {
        return null;
      }
      return (FSLeafQueue)queue;
    }
  }
  
  /**
   * Creates a leaf queue and places it in the tree. Creates any
   * parents that don't already exist.
   * 
   * @return
   *    the created queue, if successful. null if not allowed (one of the parent
   *    queues in the queue name is already a leaf queue)
   */
  private FSLeafQueue createLeafQueue(String name) {
    List<String> newQueueNames = new ArrayList<String>();
    newQueueNames.add(name);
    int sepIndex = name.length();
    FSParentQueue parent = null;

    // Move up the queue tree until we reach one that exists.
    while (sepIndex != -1) {
      sepIndex = name.lastIndexOf('.', sepIndex-1);
      FSQueue queue;
      String curName = null;
      curName = name.substring(0, sepIndex);
      queue = queues.get(curName);

      if (queue == null) {
        newQueueNames.add(curName);
      } else {
        if (queue instanceof FSParentQueue) {
          parent = (FSParentQueue)queue;
          break;
        } else {
          return null;
        }
      }
    }
    
    // At this point, parent refers to the deepest existing parent of the
    // queue to create.
    // Now that we know everything worked out, make all the queues
    // and add them to the map.
    FSLeafQueue leafQueue = null;
    for (int i = newQueueNames.size()-1; i >= 0; i--) {
      String queueName = newQueueNames.get(i);
      if (i == 0) {
        // First name added was the leaf queue
        leafQueue = new FSLeafQueue(name, this, scheduler, parent);
        parent.addChildQueue(leafQueue);
        queues.put(leafQueue.getName(), leafQueue);
        leafQueues.add(leafQueue);
      } else {
        FSParentQueue newParent = new FSParentQueue(queueName, this, scheduler, parent);
        parent.addChildQueue(newParent);
        queues.put(newParent.getName(), newParent);
        parent = newParent;
      }
    }
    
    return leafQueue;
  }

  /**
   * Gets a queue by name.
   */
  public FSQueue getQueue(String name) {
    if (!name.startsWith(ROOT_QUEUE + ".") && !name.equals(ROOT_QUEUE)) {
      name = ROOT_QUEUE + "." + name;
    }
    synchronized (queues) {
      return queues.get(name);
    }
  }

  /**
   * Return whether a queue exists already.
   */
  public boolean exists(String name) {
    if (!name.startsWith(ROOT_QUEUE + ".") && !name.equals(ROOT_QUEUE)) {
      name = ROOT_QUEUE + "." + name;
    }
    synchronized (queues) {
      return queues.containsKey(name);
    }
  }

  /**
   * Get the queue for a given AppSchedulable.
   */
  public FSLeafQueue getQueueForApp(AppSchedulable app) {
    return getLeafQueue(app.getApp().getQueueName());
  }

  /**
   * Reload allocations file if it hasn't been loaded in a while
   */
  public void reloadAllocsIfNecessary() {
    long time = scheduler.getClock().getTime();
    if (time > lastReloadAttempt + ALLOC_RELOAD_INTERVAL) {
      lastReloadAttempt = time;
      if (null == allocFile) {
        return;
      }
      try {
        // Get last modified time of alloc file depending whether it's a String
        // (for a path name) or an URL (for a classloader resource)
        long lastModified;
        if (allocFile instanceof String) {
          File file = new File((String) allocFile);
          lastModified = file.lastModified();
        } else { // allocFile is an URL
          URLConnection conn = ((URL) allocFile).openConnection();
          lastModified = conn.getLastModified();
        }
        if (lastModified > lastSuccessfulReload &&
            time > lastModified + ALLOC_RELOAD_WAIT) {
          reloadAllocs();
          lastSuccessfulReload = time;
          lastReloadAttemptFailed = false;
        }
      } catch (Exception e) {
        // Throwing the error further out here won't help - the RPC thread
        // will catch it and report it in a loop. Instead, just log it and
        // hope somebody will notice from the log.
        // We log the error only on the first failure so we don't fill up the
        // JobTracker's log with these messages.
        if (!lastReloadAttemptFailed) {
          LOG.error("Failed to reload fair scheduler config file - " +
              "will use existing allocations.", e);
        }
        lastReloadAttemptFailed = true;
      }
    }
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
  public void reloadAllocs() throws IOException, ParserConfigurationException,
      SAXException, AllocationConfigurationException {
    if (allocFile == null) return;
    // Create some temporary hashmaps to hold the new allocs, and we only save
    // them in our fields if we have parsed the entire allocs file successfully.
    Map<String, Resource> minQueueResources = new HashMap<String, Resource>();
    Map<String, Resource> maxQueueResources = new HashMap<String, Resource>();
    Map<String, Integer> queueMaxApps = new HashMap<String, Integer>();
    Map<String, Integer> userMaxApps = new HashMap<String, Integer>();
    Map<String, Double> queueWeights = new HashMap<String, Double>();
    Map<String, SchedulingMode> queueModes = new HashMap<String, SchedulingMode>();
    Map<String, Long> minSharePreemptionTimeouts = new HashMap<String, Long>();
    Map<String, Map<QueueACL, AccessControlList>> queueAcls =
        new HashMap<String, Map<QueueACL, AccessControlList>>();
    int userMaxAppsDefault = Integer.MAX_VALUE;
    int queueMaxAppsDefault = Integer.MAX_VALUE;
    long fairSharePreemptionTimeout = Long.MAX_VALUE;
    long defaultMinSharePreemptionTimeout = Long.MAX_VALUE;
    SchedulingMode defaultSchedulingMode = SchedulingMode.FAIR;

    // Remember all queue names so we can display them on web UI, etc.
    List<String> queueNamesInAllocFile = new ArrayList<String>();

    // Read and parse the allocations file.
    DocumentBuilderFactory docBuilderFactory =
      DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc;
    if (allocFile instanceof String) {
      doc = builder.parse(new File((String) allocFile));
    } else {
      doc = builder.parse(allocFile.toString());
    }
    Element root = doc.getDocumentElement();
    if (!"allocations".equals(root.getTagName()))
      throw new AllocationConfigurationException("Bad fair scheduler config " +
          "file: top-level element not <allocations>");
    NodeList elements = root.getChildNodes();
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (!(node instanceof Element))
        continue;
      Element element = (Element)node;
      if ("queue".equals(element.getTagName()) ||
    	  "pool".equals(element.getTagName())) {
        loadQueue("root", element, minQueueResources, maxQueueResources, queueMaxApps,
            userMaxApps, queueWeights, queueModes, minSharePreemptionTimeouts,
            queueAcls, queueNamesInAllocFile);
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
        queueMaxAppsDefault = val;}
      else if ("defaultQueueSchedulingMode".equals(element.getTagName())) {
        String text = ((Text)element.getFirstChild()).getData().trim();
        defaultSchedulingMode = parseSchedulingMode(text);
      } else {
        LOG.warn("Bad element in allocations file: " + element.getTagName());
      }
    }

    // Commit the reload; also create any queue defined in the alloc file
    // if it does not already exist, so it can be displayed on the web UI.
    synchronized (this) {
      info = new QueueManagerInfo(minQueueResources, maxQueueResources,
          queueMaxApps, userMaxApps, queueWeights, userMaxAppsDefault,
          queueMaxAppsDefault, defaultSchedulingMode, minSharePreemptionTimeouts,
          queueAcls, fairSharePreemptionTimeout, defaultMinSharePreemptionTimeout);
      
      // Root queue should have empty ACLs.  As a queue's ACL is the union of
      // its ACL and all its parents' ACLs, setting the roots' to empty will
      // neither allow nor prohibit more access to its children.
      Map<QueueACL, AccessControlList> rootAcls =
          new HashMap<QueueACL, AccessControlList>();
      rootAcls.put(QueueACL.SUBMIT_APPLICATIONS, new AccessControlList(" "));
      rootAcls.put(QueueACL.ADMINISTER_QUEUE, new AccessControlList(" "));
      queueAcls.put(ROOT_QUEUE, rootAcls);

      for (String name: queueNamesInAllocFile) {
        FSLeafQueue queue = getLeafQueue(name);
        if (queueModes.containsKey(name)) {
          queue.setSchedulingMode(queueModes.get(name));
        } else {
          queue.setSchedulingMode(defaultSchedulingMode);
        }
      }
    }
  }
  
  /**
   * Loads a queue from a queue element in the configuration file
   */
  private void loadQueue(String parentName, Element element, Map<String, Resource> minQueueResources,
      Map<String, Resource> maxQueueResources, Map<String, Integer> queueMaxApps,
      Map<String, Integer> userMaxApps, Map<String, Double> queueWeights,
      Map<String, SchedulingMode> queueModes, Map<String, Long> minSharePreemptionTimeouts,
      Map<String, Map<QueueACL, AccessControlList>> queueAcls, List<String> queueNamesInAllocFile) 
      throws AllocationConfigurationException {
    String queueName = parentName + "." + element.getAttribute("name");
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
        int val = Integer.parseInt(text);
        minQueueResources.put(queueName, Resources.createResource(val));
      } else if ("maxResources".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        maxQueueResources.put(queueName, Resources.createResource(val));
      } else if ("maxRunningApps".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        int val = Integer.parseInt(text);
        queueMaxApps.put(queueName, val);
      } else if ("weight".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        double val = Double.parseDouble(text);
        queueWeights.put(queueName, val);
      } else if ("minSharePreemptionTimeout".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        long val = Long.parseLong(text) * 1000L;
        minSharePreemptionTimeouts.put(queueName, val);
      } else if ("schedulingMode".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        queueModes.put(queueName, parseSchedulingMode(text));
      } else if ("aclSubmitApps".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        acls.put(QueueACL.SUBMIT_APPLICATIONS, new AccessControlList(text));
      } else if ("aclAdministerApps".equals(field.getTagName())) {
        String text = ((Text)field.getFirstChild()).getData().trim();
        acls.put(QueueACL.ADMINISTER_QUEUE, new AccessControlList(text));
      } else if ("queue".endsWith(field.getTagName()) || 
          "pool".equals(field.getTagName())) {
        loadQueue(queueName, field, minQueueResources, maxQueueResources, queueMaxApps,
            userMaxApps, queueWeights, queueModes, minSharePreemptionTimeouts,
            queueAcls, queueNamesInAllocFile);
        isLeaf = false;
      }
    }
    if (isLeaf) {
      queueNamesInAllocFile.add(queueName);
    }
    queueAcls.put(queueName, acls);
    if (maxQueueResources.containsKey(queueName) && minQueueResources.containsKey(queueName)
        && Resources.lessThan(maxQueueResources.get(queueName),
            minQueueResources.get(queueName))) {
      LOG.warn(String.format("Queue %s has max resources %d less than min resources %d",
          queueName, maxQueueResources.get(queueName), minQueueResources.get(queueName)));
    }
  }

  private SchedulingMode parseSchedulingMode(String text)
      throws AllocationConfigurationException {
    text = text.toLowerCase();
    if (text.equals("fair")) {
      return SchedulingMode.FAIR;
    } else if (text.equals("fifo")) {
      return SchedulingMode.FIFO;
    } else {
      throw new AllocationConfigurationException(
          "Unknown scheduling mode : " + text + "; expected 'fifo' or 'fair'");
    }
  }

  /**
   * Get the minimum resource allocation for the given queue.
   * @return the cap set on this queue, or 0 if not set.
   */
  public Resource getMinResources(String queue) {
    Resource minQueueResource = info.minQueueResources.get(queue);
    if (minQueueResource != null) {
      return minQueueResource;
    } else {
      return Resources.createResource(0);
    }
  }

  /**
   * Get the maximum resource allocation for the given queue.
   * @return the cap set on this queue, or Integer.MAX_VALUE if not set.
   */

  public Resource getMaxResources(String queueName) {
    Resource maxQueueResource = info.maxQueueResources.get(queueName);
    if (maxQueueResource != null) {
      return maxQueueResource;
    } else {
      return Resources.createResource(Integer.MAX_VALUE);
    }
  }

  /**
   * Get a collection of all queues
   */
  public Collection<FSLeafQueue> getLeafQueues() {
    synchronized (queues) {
      return leafQueues;
    }
  }

  public int getUserMaxApps(String user) {
    // save current info in case it gets changed under us
    QueueManagerInfo info = this.info;
    if (info.userMaxApps.containsKey(user)) {
      return info.userMaxApps.get(user);
    } else {
      return info.userMaxAppsDefault;
    }
  }

  public int getQueueMaxApps(String queue) {
    // save current info in case it gets changed under us
    QueueManagerInfo info = this.info;
    if (info.queueMaxApps.containsKey(queue)) {
      return info.queueMaxApps.get(queue);
    } else {
      return info.queueMaxAppsDefault;
    }
  }
  
  public double getQueueWeight(String queue) {
    Double weight = info.queueWeights.get(queue);
    if (weight != null) {
      return weight;
    } else {
      return 1.0;
    }
  }

  /**
   * Get a queue's min share preemption timeout, in milliseconds. This is the
   * time after which jobs in the queue may kill other queues' tasks if they
   * are below their min share.
   */
  public long getMinSharePreemptionTimeout(String queueName) {
    // save current info in case it gets changed under us
    QueueManagerInfo info = this.info;
    if (info.minSharePreemptionTimeouts.containsKey(queueName)) {
      return info.minSharePreemptionTimeouts.get(queueName);
    }
    return info.defaultMinSharePreemptionTimeout;
  }
  
  /**
   * Get the fair share preemption, in milliseconds. This is the time
   * after which any job may kill other jobs' tasks if it is below half
   * its fair share.
   */
  public long getFairSharePreemptionTimeout() {
    return info.fairSharePreemptionTimeout;
  }

  /**
   * Get the ACLs associated with this queue. If a given ACL is not explicitly
   * configured, include the default value for that ACL.
   */
  public Map<QueueACL, AccessControlList> getQueueAcls(String queue) {
    HashMap<QueueACL, AccessControlList> out = new HashMap<QueueACL, AccessControlList>();
    Map<QueueACL, AccessControlList> queueAcl = info.queueAcls.get(queue);
    if (queueAcl != null) {
      out.putAll(queueAcl);
    }
    if (!out.containsKey(QueueACL.ADMINISTER_QUEUE)) {
      out.put(QueueACL.ADMINISTER_QUEUE, new AccessControlList("*"));
    }
    if (!out.containsKey(QueueACL.SUBMIT_APPLICATIONS)) {
      out.put(QueueACL.SUBMIT_APPLICATIONS, new AccessControlList("*"));
    }
    return out;
  }
  
  static class QueueManagerInfo {
    // Minimum resource allocation for each queue
    public final Map<String, Resource> minQueueResources;
    // Maximum amount of resources per queue
    public final Map<String, Resource> maxQueueResources;
    // Sharing weights for each queue
    public final Map<String, Double> queueWeights;
    
    // Max concurrent running applications for each queue and for each user; in addition,
    // for users that have no max specified, we use the userMaxJobsDefault.
    public final Map<String, Integer> queueMaxApps;
    public final Map<String, Integer> userMaxApps;
    public final int userMaxAppsDefault;
    public final int queueMaxAppsDefault;

    // ACL's for each queue. Only specifies non-default ACL's from configuration.
    public final Map<String, Map<QueueACL, AccessControlList>> queueAcls;

    // Min share preemption timeout for each queue in seconds. If a job in the queue
    // waits this long without receiving its guaranteed share, it is allowed to
    // preempt other jobs' tasks.
    public final Map<String, Long> minSharePreemptionTimeouts;

    // Default min share preemption timeout for queues where it is not set
    // explicitly.
    public final long defaultMinSharePreemptionTimeout;

    // Preemption timeout for jobs below fair share in seconds. If a job remains
    // below half its fair share for this long, it is allowed to preempt tasks.
    public final long fairSharePreemptionTimeout;

    public final SchedulingMode defaultSchedulingMode;
    
    public QueueManagerInfo(Map<String, Resource> minQueueResources, 
        Map<String, Resource> maxQueueResources, 
        Map<String, Integer> queueMaxApps, Map<String, Integer> userMaxApps,
        Map<String, Double> queueWeights, int userMaxAppsDefault,
        int queueMaxAppsDefault, SchedulingMode defaultSchedulingMode, 
        Map<String, Long> minSharePreemptionTimeouts, 
        Map<String, Map<QueueACL, AccessControlList>> queueAcls,
        long fairSharePreemptionTimeout, long defaultMinSharePreemptionTimeout) {
      this.minQueueResources = minQueueResources;
      this.maxQueueResources = maxQueueResources;
      this.queueMaxApps = queueMaxApps;
      this.userMaxApps = userMaxApps;
      this.queueWeights = queueWeights;
      this.userMaxAppsDefault = userMaxAppsDefault;
      this.queueMaxAppsDefault = queueMaxAppsDefault;
      this.defaultSchedulingMode = defaultSchedulingMode;
      this.minSharePreemptionTimeouts = minSharePreemptionTimeouts;
      this.queueAcls = queueAcls;
      this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
      this.defaultMinSharePreemptionTimeout = defaultMinSharePreemptionTimeout;
    }
    
    public QueueManagerInfo() {
      minQueueResources = new HashMap<String, Resource>();
      maxQueueResources = new HashMap<String, Resource>();
      queueWeights = new HashMap<String, Double>();
      queueMaxApps = new HashMap<String, Integer>();
      userMaxApps = new HashMap<String, Integer>();
      userMaxAppsDefault = Integer.MAX_VALUE;
      queueMaxAppsDefault = Integer.MAX_VALUE;
      queueAcls = new HashMap<String, Map<QueueACL, AccessControlList>>();
      minSharePreemptionTimeouts = new HashMap<String, Long>();
      defaultMinSharePreemptionTimeout = Long.MAX_VALUE;
      fairSharePreemptionTimeout = Long.MAX_VALUE;
      defaultSchedulingMode = SchedulingMode.FAIR;
    }
  }
}
