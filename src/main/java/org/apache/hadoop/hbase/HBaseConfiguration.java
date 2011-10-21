/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.VersionInfo;

/**
 * Adds HBase configuration files to a Configuration
 */
public class HBaseConfiguration extends Configuration {

  private static final Log LOG = LogFactory.getLog(HBaseConfiguration.class);

  // a constant to convert a fraction to a percentage
  private static final int CONVERT_TO_PERCENTAGE = 100;

  /**
   * Instantinating HBaseConfiguration() is deprecated. Please use
   * HBaseConfiguration#create() to construct a plain Configuration
   */
  @Deprecated
  public HBaseConfiguration() {
    //TODO:replace with private constructor, HBaseConfiguration should not extend Configuration
    super();
    addHbaseResources(this);
    LOG.warn("instantiating HBaseConfiguration() is deprecated. Please use" +
    		" HBaseConfiguration#create() to construct a plain Configuration");
  }

  /**
   * Instantiating HBaseConfiguration() is deprecated. Please use
   * HBaseConfiguration#create(conf) to construct a plain Configuration
   */
  @Deprecated
  public HBaseConfiguration(final Configuration c) {
    //TODO:replace with private constructor
    this();
    merge(this, c);
  }

  private static void checkDefaultsVersion(Configuration conf) {
    if (conf.getBoolean("hbase.defaults.for.version.skip", Boolean.FALSE)) return;
    String defaultsVersion = conf.get("hbase.defaults.for.version");
    String thisVersion = VersionInfo.getVersion();
    if (!thisVersion.equals(defaultsVersion)) {
      throw new RuntimeException(
        "hbase-default.xml file seems to be for and old version of HBase (" +
        defaultsVersion + "), this version is " + thisVersion);
    }
  }

  private static void checkForClusterFreeMemoryLimit(Configuration conf) {
      float globalMemstoreLimit = conf.getFloat("hbase.regionserver.global.memstore.upperLimit", 0.4f);
      int gml = (int)(globalMemstoreLimit * CONVERT_TO_PERCENTAGE);
      float blockCacheUpperLimit = conf.getFloat("hfile.block.cache.size", 0.2f);
      int bcul = (int)(blockCacheUpperLimit * CONVERT_TO_PERCENTAGE);
      if (CONVERT_TO_PERCENTAGE - (gml + bcul)
              < (int)(CONVERT_TO_PERCENTAGE * 
                      HConstants.HBASE_CLUSTER_MINIMUM_MEMORY_THRESHOLD)) {
          throw new RuntimeException(
            "Current heap configuration for MemStore and BlockCache exceeds " +
            "the threshold required for successful cluster operation. " +
            "The combined value cannot exceed 0.8. Please check " +
            "the settings for hbase.regionserver.global.memstore.upperLimit and " +
            "hfile.block.cache.size in your configuration. " +
            "hbase.regionserver.global.memstore.upperLimit is " + 
            globalMemstoreLimit +
            " hfile.block.cache.size is " + blockCacheUpperLimit);
      }
  }

  public static Configuration addHbaseResources(Configuration conf) {
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");

    checkDefaultsVersion(conf);
    checkForClusterFreeMemoryLimit(conf);
    return conf;
  }

  /**
   * Creates a Configuration with HBase resources
   * @return a Configuration with HBase resources
   */
  public static Configuration create() {
    Configuration conf = new Configuration();
    return addHbaseResources(conf);
  }

  /**
   * Creates a clone of passed configuration.
   * @param that Configuration to clone.
   * @return a Configuration created with the hbase-*.xml files plus
   * the given configuration.
   */
  public static Configuration create(final Configuration that) {
    Configuration conf = create();
    merge(conf, that);
    return conf;
  }

  /**
   * Merge two configurations.
   * @param destConf the configuration that will be overwritten with items
   *                 from the srcConf
   * @param srcConf the source configuration
   **/
  public static void merge(Configuration destConf, Configuration srcConf) {
    for (Entry<String, String> e : srcConf) {
      destConf.set(e.getKey(), e.getValue());
    }
  }
}
