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

package org.apache.hadoop.raid;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import org.apache.hadoop.raid.protocol.PolicyInfo;
import org.apache.hadoop.raid.protocol.PolicyList;

/**
 * Maintains the configuration xml file that is read into memory.
 */
class ConfigManager {
  public static final Log LOG = LogFactory.getLog(
    "org.apache.hadoop.raid.ConfigManager");

  /** Time to wait between checks of the config file */
  public static final long RELOAD_INTERVAL = 10 * 1000;

  /** Time to wait between successive runs of all policies */
  public static final long RESCAN_INTERVAL = 3600 * 1000;
  
  /**
   * Time to wait after the config file has been modified before reloading it
   * (this is done to prevent loading a file that hasn't been fully written).
   */
  public static final long RELOAD_WAIT = 5 * 1000; 
  
  private Configuration conf;    // Hadoop configuration
  private String configFileName; // Path to config XML file
  
  private long lastReloadAttempt; // Last time we tried to reload the config file
  private long lastSuccessfulReload; // Last time we successfully reloaded config
  private boolean lastReloadAttemptFailed = false;
  private long reloadInterval = RELOAD_INTERVAL;
  private long periodicity; // time between runs of all policies

  // Reload the configuration
  private boolean doReload;
  private Thread reloadThread;
  private volatile boolean running = false;

  // Collection of all configured policies.
  Collection<PolicyList> allPolicies = new ArrayList<PolicyList>();

  public ConfigManager(Configuration conf) throws IOException, SAXException,
      RaidConfigurationException, ClassNotFoundException, ParserConfigurationException {
    this.conf = conf;
    this.configFileName = conf.get("raid.config.file");
    this.doReload = conf.getBoolean("raid.config.reload", true);
    this.reloadInterval = conf.getLong("raid.config.reload.interval", RELOAD_INTERVAL);
    this.periodicity = conf.getLong("raid.policy.rescan.interval",  RESCAN_INTERVAL);
    if (configFileName == null) {
      String msg = "No raid.config.file given in conf - " +
                   "the Hadoop Raid utility cannot run. Aborting....";
      LOG.warn(msg);
      throw new IOException(msg);
    }
    reloadConfigs();
    lastSuccessfulReload = RaidNode.now();
    lastReloadAttempt = RaidNode.now();
    running = true;
  }
  
  /**
   * Reload config file if it hasn't been loaded in a while
   * Returns true if the file was reloaded.
   */
  public synchronized boolean reloadConfigsIfNecessary() {
    long time = RaidNode.now();
    if (time > lastReloadAttempt + reloadInterval) {
      lastReloadAttempt = time;
      try {
        File file = new File(configFileName);
        long lastModified = file.lastModified();
        if (lastModified > lastSuccessfulReload &&
            time > lastModified + RELOAD_WAIT) {
          reloadConfigs();
          lastSuccessfulReload = time;
          lastReloadAttemptFailed = false;
          return true;
        }
      } catch (Exception e) {
        if (!lastReloadAttemptFailed) {
          LOG.error("Failed to reload config file - " +
              "will use existing configuration.", e);
        }
        lastReloadAttemptFailed = true;
      }
    }
    return false;
  }
  
  /**
   * Updates the in-memory data structures from the config file. This file is
   * expected to be in the following whitespace-separated format:
   * 
   <configuration>
    <srcPath prefix="hdfs://hadoop.myhost.com:9000/user/warehouse/u_full/*">
      <destPath> hdfs://dfsarch.data.facebook.com:9000/archive/</destPath>
      <policy name = RaidScanWeekly>
        <property>
          <name>targetReplication</name>
          <value>2</value>
          <description> after RAIDing, decrease the replication factor of the file to 
                        this value.
          </description>
        </property>
        <property>
          <name>metaReplication</name>
          <value>2</value>
          <description> the replication factor of the RAID meta file
          </description>
        </property>
        <property>
          <name>stripeLength</name>
          <value>10</value>
          <description> the number of blocks to RAID together
          </description>
        </property>
      </policy>
    </srcPath>
   </configuration>
   *
   * Blank lines and lines starting with # are ignored.
   *  
   * @throws IOException if the config file cannot be read.
   * @throws RaidConfigurationException if configuration entries are invalid.
   * @throws ClassNotFoundException if user-defined policy classes cannot be loaded
   * @throws ParserConfigurationException if XML parser is misconfigured.
   * @throws SAXException if config file is malformed.
   * @returns A new set of policy categories.
   */
  void reloadConfigs() throws IOException, ParserConfigurationException, 
      SAXException, ClassNotFoundException, RaidConfigurationException {

    if (configFileName == null) {
       return;
    }
    
    File file = new File(configFileName);
    if (!file.exists()) {
      throw new RaidConfigurationException("Configuration file " + configFileName +
                                           " does not exist.");
    }

    // Create some temporary hashmaps to hold the new allocs, and we only save
    // them in our fields if we have parsed the entire allocs file successfully.
    List<PolicyList> all = new ArrayList<PolicyList>();
    long periodicityValue = periodicity;
    
    
    // Read and parse the configuration file.
    // allow include files in configuration file
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    docBuilderFactory.setIgnoringComments(true);
    docBuilderFactory.setNamespaceAware(true);
    try {
      docBuilderFactory.setXIncludeAware(true);
    } catch (UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for raid parser "
                + docBuilderFactory + ":" + e, e);
    }
    LOG.error("Reloading config file " + file);

    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = builder.parse(file);
    Element root = doc.getDocumentElement();
    if (!"configuration".equalsIgnoreCase(root.getTagName()))
      throw new RaidConfigurationException("Bad configuration file: " + 
          "top-level element not <configuration>");
    NodeList elements = root.getChildNodes();

    // loop through all the configured source paths.
    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (!(node instanceof Element)) {
        continue;
      }
      Element element = (Element)node;
      String elementTagName = element.getTagName();
      if ("srcPath".equalsIgnoreCase(elementTagName)) {
        String srcPathPrefix = element.getAttribute("prefix");

        if (srcPathPrefix == null || srcPathPrefix.length() == 0) {
          throw new RaidConfigurationException("Bad configuration file: " + 
             "srcPathPrefix not set.");
        }
        PolicyList policyList = new PolicyList();
        all.add(policyList);

        policyList.setSrcPath(conf, srcPathPrefix);
        
        // loop through all the policies for this source path
        NodeList policies = element.getChildNodes();
        for (int j = 0; j < policies.getLength(); j++) {
          Node node1 = policies.item(j);
          if (!(node1 instanceof Element)) {
            continue;
          }
          Element policy = (Element)node1;
          if (!"policy".equalsIgnoreCase(policy.getTagName())) {
            throw new RaidConfigurationException("Bad configuration file: " + 
              "Expecting <policy> for srcPath " + srcPathPrefix);
          }
          String policyName = policy.getAttribute("name");
          PolicyInfo pinfo = new PolicyInfo(policyName, conf);
          pinfo.setSrcPath(srcPathPrefix);
          policyList.add(pinfo);

          // loop through all the properties of this policy
          NodeList properties = policy.getChildNodes();
          for (int k = 0; k < properties.getLength(); k++) {
            Node node2 = properties.item(k);
            if (!(node2 instanceof Element)) {
              continue;
            }
            Element property = (Element)node2;
            String propertyName = property.getTagName();
            if ("destPath".equalsIgnoreCase(propertyName)) {
              String text = ((Text)property.getFirstChild()).getData().trim();
              LOG.info(policyName + ".destPath = " + text);
              pinfo.setDestinationPath(text);
            } else if ("description".equalsIgnoreCase(propertyName)) {
              String text = ((Text)property.getFirstChild()).getData().trim();
              pinfo.setDescription(text);
            } else if ("property".equalsIgnoreCase(propertyName)) {
              NodeList nl = property.getChildNodes();
              String pname=null,pvalue=null;
              for (int l = 0; l < nl.getLength(); l++){
                Node node3 = nl.item(l);
                if (!(node3 instanceof Element)) {
                  continue;
                }
                Element item = (Element) node3;
                String itemName = item.getTagName();
                if ("name".equalsIgnoreCase(itemName)){
                  pname = ((Text)item.getFirstChild()).getData().trim();
                } else if ("value".equalsIgnoreCase(itemName)){
                  pvalue = ((Text)item.getFirstChild()).getData().trim();
                }
              }
              if (pname != null && pvalue != null) {
                LOG.info(policyName + "." + pname + " = " + pvalue);
                pinfo.setProperty(pname,pvalue);
              }
            } else {
              LOG.info("Found bad property " + propertyName +
                       " for srcPath" + srcPathPrefix +
                       " policy name " + policyName +
                       ". Ignoring."); 
            }
          }  // done with all properties of this policy
        }    // done with all policies for this srcpath
      } 
    }        // done with all srcPaths
    setAllPolicies(all);
    periodicity = periodicityValue;
    return;
  }


  public synchronized long getPeriodicity() {
    return periodicity;
  }
  
  /**
   * Get a collection of all policies
   */
  public synchronized Collection<PolicyList> getAllPolicies() {
    return new ArrayList(allPolicies);
  }
  
  /**
   * Set a collection of all policies
   */
  protected synchronized void setAllPolicies(Collection<PolicyList> value) {
    this.allPolicies = value;
  }

  /**
   * Start a background thread to reload the config file
   */
  void startReload() {
    if (doReload) {
      reloadThread = new UpdateThread();
      reloadThread.start();
    }
  }

  /**
   * Stop the background thread that reload the config file
   */
  void stopReload() throws InterruptedException {
    if (reloadThread != null) {
      running = false;
      reloadThread.interrupt();
      reloadThread.join();
      reloadThread = null;
    }
  }

  /**
   * A thread which reloads the config file.
   */
  private class UpdateThread extends Thread {
    private UpdateThread() {
      super("Raid update thread");
    }

    public void run() {
      while (running) {
        try {
          Thread.sleep(reloadInterval);
          reloadConfigsIfNecessary();
        } catch (InterruptedException e) {
          // do nothing 
        } catch (Exception e) {
          LOG.error("Failed to reload config file ", e);
        }
      }
    }
  }
}
