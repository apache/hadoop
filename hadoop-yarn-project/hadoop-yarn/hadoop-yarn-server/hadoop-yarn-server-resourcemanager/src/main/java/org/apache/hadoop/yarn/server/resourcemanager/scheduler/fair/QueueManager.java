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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

/**
 * Maintains a list of queues as well as scheduling parameters for each queue,
 * such as guaranteed share allocations, from the fair scheduler config file.
 */
@Private
@Unstable
public class QueueManager {
  public static final Log LOG = LogFactory.getLog(
    QueueManager.class.getName());

  /** Time to wait between checks of the allocation file */
  public static final long ALLOC_RELOAD_INTERVAL = 10 * 1000;

  /**
   * Time to wait after the allocation has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long ALLOC_RELOAD_WAIT = 5 * 1000;

  private final FairScheduler scheduler;

  // Minimum resource allocation for each queue
  private Map<String, Resource> minQueueResources = new HashMap<String, Resource>();
  // Maximum amount of resources per queue
  private Map<String, Resource> maxQueueResources = new HashMap<String, Resource>();
  // Sharing weights for each queue
  private Map<String, Double> queueWeights = new HashMap<String, Double>();

  // Max concurrent running applications for each queue and for each user; in addition,
  // for users that have no max specified, we use the userMaxJobsDefault.
  private Map<String, Integer> queueMaxApps = new HashMap<String, Integer>();
  private Map<String, Integer> userMaxApps = new HashMap<String, Integer>();
  private int userMaxAppsDefault = Integer.MAX_VALUE;
  private int queueMaxAppsDefault = Integer.MAX_VALUE;

  // ACL's for each queue. Only specifies non-default ACL's from configuration.
  private Map<String, Map<QueueACL, AccessControlList>> queueAcls =
      new HashMap<String, Map<QueueACL, AccessControlList>>();

  // Min share preemption timeout for each queue in seconds. If a job in the queue
  // waits this long without receiving its guaranteed share, it is allowed to
  // preempt other jobs' tasks.
  private Map<String, Long> minSharePreemptionTimeouts =
    new HashMap<String, Long>();

  // Default min share preemption timeout for queues where it is not set
  // explicitly.
  private long defaultMinSharePreemptionTimeout = Long.MAX_VALUE;

  // Preemption timeout for jobs below fair share in seconds. If a job remains
  // below half its fair share for this long, it is allowed to preempt tasks.
  private long fairSharePreemptionTimeout = Long.MAX_VALUE;

  SchedulingMode defaultSchedulingMode = SchedulingMode.FAIR;

  private Object allocFile; // Path to XML file containing allocations. This
                            // is either a URL to specify a classpath resource
                            // (if the fair-scheduler.xml on the classpath is
                            // used) or a String to specify an absolute path (if
                            // mapred.fairscheduler.allocation.file is used).

  private Map<String, FSQueue> queues = new HashMap<String, FSQueue>();

  private long lastReloadAttempt; // Last time we tried to reload the queues file
  private long lastSuccessfulReload; // Last time we successfully reloaded queues
  private boolean lastReloadAttemptFailed = false;
  
  // Monitor object for minQueueResources
  private Object minQueueResourcesMO = new Object();
  
  //Monitor object for maxQueueResources
  private Object maxQueueResourcesMO = new Object();
  
  //Monitor object for queueMaxApps
  private Object queueMaxAppsMO = new Object();
  
  //Monitor object for userMaxApps
  private Object userMaxAppsMO = new Object();
  
  //Monitor object for queueWeights
  private Object queueWeightsMO = new Object();
  
  //Monitor object for minSharePreemptionTimeouts
  private Object minSharePreemptionTimeoutsMO = new Object();
  
  //Monitor object for queueAcls
  private Object queueAclsMO = new Object();
  
  //Monitor object for userMaxAppsDefault
  private Object userMaxAppsDefaultMO = new Object();
  
  //Monitor object for queueMaxAppsDefault
  private Object queueMaxAppsDefaultMO = new Object();
  
  //Monitor object for defaultSchedulingMode
  private Object defaultSchedulingModeMO = new Object();
  
  public QueueManager(FairScheduler scheduler) {
    this.scheduler = scheduler;
  }

  public void initialize() throws IOException, SAXException,
      AllocationConfigurationException, ParserConfigurationException {
    FairSchedulerConfiguration conf = scheduler.getConf();
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
    getQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
  }

  /**
   * Get a queue by name, creating it if necessary
   */
  public FSQueue getQueue(String name) {
    synchronized (queues) {
      FSQueue queue = queues.get(name);
      if (queue == null) {
        queue = new FSQueue(scheduler, name);
        synchronized (defaultSchedulingModeMO){
          queue.setSchedulingMode(defaultSchedulingMode);
        }
        queues.put(name, queue);
      }
      return queue;
    }
  }

  /**
   * Return whether a queue exists already.
   */
  public boolean exists(String name) {
    synchronized (queues) {
      return queues.containsKey(name);
    }
  }

  /**
   * Get the queue for a given AppSchedulable.
   */
  public FSQueue getQueueForApp(AppSchedulable app) {
    return this.getQueue(app.getApp().getQueueName());
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
        String queueName = element.getAttribute("name");
        Map<QueueACL, AccessControlList> acls =
            new HashMap<QueueACL, AccessControlList>();
        queueNamesInAllocFile.add(queueName);
        NodeList fields = element.getChildNodes();
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
          }
        }
        queueAcls.put(queueName, acls);
        if (maxQueueResources.containsKey(queueName) && minQueueResources.containsKey(queueName)
            && Resources.lessThan(maxQueueResources.get(queueName),
                minQueueResources.get(queueName))) {
          LOG.warn(String.format("Queue %s has max resources %d less than min resources %d",
              queueName, maxQueueResources.get(queueName), minQueueResources.get(queueName)));
        }
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
    synchronized(this) {
      setMinResources(minQueueResources);
      setMaxResources(maxQueueResources);
      setQueueMaxApps(queueMaxApps);
      setUserMaxApps(userMaxApps);
      setQueueWeights(queueWeights);
      setUserMaxAppsDefault(userMaxAppsDefault);
      setQueueMaxAppsDefault(queueMaxAppsDefault);
      setDefaultSchedulingMode(defaultSchedulingMode);
      setMinSharePreemptionTimeouts(minSharePreemptionTimeouts);
      setQueueAcls(queueAcls);
      for (String name: queueNamesInAllocFile) {
        FSQueue queue = getQueue(name);
        if (queueModes.containsKey(name)) {
          queue.setSchedulingMode(queueModes.get(name));
        } else {
          queue.setSchedulingMode(defaultSchedulingMode);
        }
      }
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
    synchronized(minQueueResourcesMO) {
      if (minQueueResources.containsKey(queue)) {
        return minQueueResources.get(queue);
      } else{
        return Resources.createResource(0);
      }
    }
  }

  private void setMinResources(Map<String, Resource> resources) {
    synchronized(minQueueResourcesMO) {
      minQueueResources = resources;
    }
  }
  /**
   * Get the maximum resource allocation for the given queue.
   * @return the cap set on this queue, or Integer.MAX_VALUE if not set.
   */
  Resource getMaxResources(String queueName) {
    synchronized (maxQueueResourcesMO) {
      if (maxQueueResources.containsKey(queueName)) {
        return maxQueueResources.get(queueName);
      } else {
        return Resources.createResource(Integer.MAX_VALUE);
      }
    }
  }

  private void setMaxResources(Map<String, Resource> resources) {
    synchronized(maxQueueResourcesMO) {
      maxQueueResources = resources;
    }
  }
  
  /**
   * Add an app in the appropriate queue
   */
  public synchronized void addApp(FSSchedulerApp app) {
    getQueue(app.getQueueName()).addApp(app);
  }

  /**
   * Remove an app
   */
  public synchronized void removeJob(FSSchedulerApp app) {
    getQueue(app.getQueueName()).removeJob(app);
  }

  /**
   * Get a collection of all queues
   */
  public Collection<FSQueue> getQueues() {
    synchronized (queues) {
      return queues.values();
    }
  }

  /**
   * Get all queue names that have been seen either in the allocation file or in
   * a submitted app.
   */
  public synchronized Collection<String> getQueueNames() {
    List<String> list = new ArrayList<String>();
    for (FSQueue queue: getQueues()) {
      list.add(queue.getName());
    }
    Collections.sort(list);
    return list;
  }

  public int getUserMaxApps(String user) {
    synchronized (userMaxAppsMO) {
      if (userMaxApps.containsKey(user)) {
        return userMaxApps.get(user);
      } else {
        return getUserMaxAppsDefault();
      }
    }
  }

  private void setUserMaxApps(Map<String, Integer> userApps) {
    synchronized (userMaxAppsMO) {
      userMaxApps = userApps;
    }
  }
  
  private int getUserMaxAppsDefault() {
    synchronized (userMaxAppsDefaultMO){
      return userMaxAppsDefault;
    }
  }
  
  private void setUserMaxAppsDefault(int userMaxApps) {
    synchronized (userMaxAppsDefaultMO){
      userMaxAppsDefault = userMaxApps;
    }
  }
  
  public int getQueueMaxApps(String queue) {
    synchronized (queueMaxAppsMO) {
      if (queueMaxApps.containsKey(queue)) {
        return queueMaxApps.get(queue);
      } else {
        return getQueueMaxAppsDefault();
      }
    }
  }
  
  private void setQueueMaxApps(Map<String, Integer> queueApps) {
    synchronized (queueMaxAppsMO) {
      queueMaxApps = queueApps;
    }
  }
  
  private int getQueueMaxAppsDefault(){
    synchronized(queueMaxAppsDefaultMO) {
      return queueMaxAppsDefault;
    }
  }
  
  private void setQueueMaxAppsDefault(int queueMaxApps){
    synchronized(queueMaxAppsDefaultMO) {
      queueMaxAppsDefault = queueMaxApps;
    }
  }
  
  private void setDefaultSchedulingMode(SchedulingMode schedulingMode){
    synchronized(defaultSchedulingModeMO) {
      defaultSchedulingMode = schedulingMode;
    }
  }

  public double getQueueWeight(String queue) {
    synchronized (queueWeightsMO) {
      if (queueWeights.containsKey(queue)) {
        return queueWeights.get(queue);
      } else {
        return 1.0;
      }
    }
  }

  private void setQueueWeights(Map<String, Double> weights) {
    synchronized (queueWeightsMO) {
      queueWeights = weights;
    }
  }
  /**
  * Get a queue's min share preemption timeout, in milliseconds. This is the
  * time after which jobs in the queue may kill other queues' tasks if they
  * are below their min share.
  */
  public long getMinSharePreemptionTimeout(String queueName) {
    synchronized (minSharePreemptionTimeoutsMO) {
      if (minSharePreemptionTimeouts.containsKey(queueName)) {
        return minSharePreemptionTimeouts.get(queueName);
      }
    }
    return defaultMinSharePreemptionTimeout;
  }
  
  private void setMinSharePreemptionTimeouts(
      Map<String, Long> sharePreemptionTimeouts){
    synchronized (minSharePreemptionTimeoutsMO) {
      minSharePreemptionTimeouts = sharePreemptionTimeouts;
    }
  }

  /**
   * Get the fair share preemption, in milliseconds. This is the time
   * after which any job may kill other jobs' tasks if it is below half
   * its fair share.
   */
  public long getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  /**
   * Get the ACLs associated with this queue. If a given ACL is not explicitly
   * configured, include the default value for that ACL.
   */
  public Map<QueueACL, AccessControlList> getQueueAcls(String queue) {
    HashMap<QueueACL, AccessControlList> out = new HashMap<QueueACL, AccessControlList>();
    synchronized (queueAclsMO) {
      if (queueAcls.containsKey(queue)) {
        out.putAll(queueAcls.get(queue));
      }
    }
    if (!out.containsKey(QueueACL.ADMINISTER_QUEUE)) {
      out.put(QueueACL.ADMINISTER_QUEUE, new AccessControlList("*"));
    }
    if (!out.containsKey(QueueACL.SUBMIT_APPLICATIONS)) {
      out.put(QueueACL.SUBMIT_APPLICATIONS, new AccessControlList("*"));
    }
    return out;
  }
  
  private void setQueueAcls(Map<String, Map<QueueACL, AccessControlList>> queue) {
    synchronized (queueAclsMO) {
      queueAcls = queue;
    }
  }
}
