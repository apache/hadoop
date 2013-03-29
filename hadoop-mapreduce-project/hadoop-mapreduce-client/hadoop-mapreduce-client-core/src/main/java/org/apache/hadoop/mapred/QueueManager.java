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

package org.apache.hadoop.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.MRConfig;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.net.URL;


/**
 * Class that exposes information about queues maintained by the Hadoop
 * Map/Reduce framework.
 * <p/>
 * The Map/Reduce framework can be configured with one or more queues,
 * depending on the scheduler it is configured with. While some
 * schedulers work only with one queue, some schedulers support multiple
 * queues. Some schedulers also support the notion of queues within
 * queues - a feature called hierarchical queues.
 * <p/>
 * Queue names are unique, and used as a key to lookup queues. Hierarchical
 * queues are named by a 'fully qualified name' such as q1:q2:q3, where
 * q2 is a child queue of q1 and q3 is a child queue of q2.
 * <p/>
 * Leaf level queues are queues that contain no queues within them. Jobs
 * can be submitted only to leaf level queues.
 * <p/>
 * Queues can be configured with various properties. Some of these
 * properties are common to all schedulers, and those are handled by this
 * class. Schedulers might also associate several custom properties with
 * queues. These properties are parsed and maintained per queue by the
 * framework. If schedulers need more complicated structure to maintain
 * configuration per queue, they are free to not use the facilities
 * provided by the framework, but define their own mechanisms. In such cases,
 * it is likely that the name of the queue will be used to relate the
 * common properties of a queue with scheduler specific properties.
 * <p/>
 * Information related to a queue, such as its name, properties, scheduling
 * information and children are exposed by this class via a serializable
 * class called {@link JobQueueInfo}.
 * <p/>
 * Queues are configured in the configuration file mapred-queues.xml.
 * To support backwards compatibility, queues can also be configured
 * in mapred-site.xml. However, when configured in the latter, there is
 * no support for hierarchical queues.
 */
@InterfaceAudience.Private
public class QueueManager {

  private static final Log LOG = LogFactory.getLog(QueueManager.class);

  // Map of a queue name and Queue object
  private Map<String, Queue> leafQueues = new HashMap<String,Queue>();
  private Map<String, Queue> allQueues = new HashMap<String, Queue>();
  public static final String QUEUE_CONF_FILE_NAME = "mapred-queues.xml";
  static final String QUEUE_CONF_DEFAULT_FILE_NAME = "mapred-queues-default.xml";

  //Prefix in configuration for queue related keys
  static final String QUEUE_CONF_PROPERTY_NAME_PREFIX = "mapred.queue.";

  //Resource in which queue acls are configured.
  private Queue root = null;
  
  // represents if job and queue acls are enabled on the mapreduce cluster
  private boolean areAclsEnabled = false;

  /**
   * Factory method to create an appropriate instance of a queue
   * configuration parser.
   * <p/>
   * Returns a parser that can parse either the deprecated property
   * style queue configuration in mapred-site.xml, or one that can
   * parse hierarchical queues in mapred-queues.xml. First preference
   * is given to configuration in mapred-site.xml. If no queue
   * configuration is found there, then a parser that can parse
   * configuration in mapred-queues.xml is created.
   *
   * @param conf Configuration instance that determines which parser
   *             to use.
   * @return Queue configuration parser
   */
  static QueueConfigurationParser getQueueConfigurationParser(
    Configuration conf, boolean reloadConf, boolean areAclsEnabled) {
    if (conf != null && conf.get(
      DeprecatedQueueConfigurationParser.MAPRED_QUEUE_NAMES_KEY) != null) {
      if (reloadConf) {
        conf.reloadConfiguration();
      }
      return new DeprecatedQueueConfigurationParser(conf);
    } else {
      URL xmlInUrl =
        Thread.currentThread().getContextClassLoader()
          .getResource(QUEUE_CONF_FILE_NAME);
      if (xmlInUrl == null) {
        xmlInUrl = Thread.currentThread().getContextClassLoader()
          .getResource(QUEUE_CONF_DEFAULT_FILE_NAME);
        assert xmlInUrl != null; // this should be in our jar
      }
      InputStream stream = null;
      try {
        stream = xmlInUrl.openStream();
        return new QueueConfigurationParser(new BufferedInputStream(stream),
            areAclsEnabled);
      } catch (IOException ioe) {
        throw new RuntimeException("Couldn't open queue configuration at " +
                                   xmlInUrl, ioe);
      } finally {
        IOUtils.closeStream(stream);
      }
    }
  }

  QueueManager() {// acls are disabled
    this(false);
  }

  QueueManager(boolean areAclsEnabled) {
    this.areAclsEnabled = areAclsEnabled;
    initialize(getQueueConfigurationParser(null, false, areAclsEnabled));
  }

  /**
   * Construct a new QueueManager using configuration specified in the passed
   * in {@link org.apache.hadoop.conf.Configuration} object.
   * <p/>
   * This instance supports queue configuration specified in mapred-site.xml,
   * but without support for hierarchical queues. If no queue configuration
   * is found in mapred-site.xml, it will then look for site configuration
   * in mapred-queues.xml supporting hierarchical queues.
   *
   * @param clusterConf    mapreduce cluster configuration
   */
  public QueueManager(Configuration clusterConf) {
    areAclsEnabled = clusterConf.getBoolean(MRConfig.MR_ACLS_ENABLED, false);
    initialize(getQueueConfigurationParser(clusterConf, false, areAclsEnabled));
  }

  /**
   * Create an instance that supports hierarchical queues, defined in
   * the passed in configuration file.
   * <p/>
   * This is mainly used for testing purposes and should not called from
   * production code.
   *
   * @param confFile File where the queue configuration is found.
   */
  QueueManager(String confFile, boolean areAclsEnabled) {
    this.areAclsEnabled = areAclsEnabled;
    QueueConfigurationParser cp =
        new QueueConfigurationParser(confFile, areAclsEnabled);
    initialize(cp);
  }

  /**
   * Initialize the queue-manager with the queue hierarchy specified by the
   * given {@link QueueConfigurationParser}.
   * 
   * @param cp
   */
  private void initialize(QueueConfigurationParser cp) {
    this.root = cp.getRoot();
    leafQueues.clear();
    allQueues.clear();
    //At this point we have root populated
    //update data structures leafNodes.
    leafQueues = getRoot().getLeafQueues();
    allQueues.putAll(getRoot().getInnerQueues());
    allQueues.putAll(leafQueues);

    LOG.info("AllQueues : " + allQueues + "; LeafQueues : " + leafQueues);
  }

 
 
  static final String MSG_REFRESH_FAILURE_WITH_CHANGE_OF_HIERARCHY =
      "Unable to refresh queues because queue-hierarchy changed. "
          + "Retaining existing configuration. ";

  static final String MSG_REFRESH_FAILURE_WITH_SCHEDULER_FAILURE =
      "Scheduler couldn't refresh it's queues with the new"
          + " configuration properties. "
          + "Retaining existing configuration throughout the system.";

 

  // this method is for internal use only
  public static final String toFullPropertyName(
    String queue,
    String property) {
    return QUEUE_CONF_PROPERTY_NAME_PREFIX + queue + "." + property;
  }

  
 
 
 

  /**
   * Used only for test.
   *
   * @return
   */
  Queue getRoot() {
    return root;
  }

  

}
