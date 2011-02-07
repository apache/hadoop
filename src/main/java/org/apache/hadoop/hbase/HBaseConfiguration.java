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

import java.net.URL;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Adds HBase configuration files to a Configuration
 */
public class HBaseConfiguration extends Configuration {

  private static final Log LOG = LogFactory.getLog(HBaseConfiguration.class);

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
    for (Entry<String, String>e: c) {
      set(e.getKey(), e.getValue());
    }
  }

  /**
   * Check that the hbase-defaults.xml file is being loaded from within
   * the hbase jar, rather than somewhere else on the classpath.
   */
  private static void checkDefaultsInJar(Configuration conf) {
    ClassLoader cl = conf.getClassLoader();
    URL url = cl.getResource("hbase-default.xml");
    if (url == null) {
      // This is essentially an assertion failure - we compile this
      // into our own jar, so there's something really wacky about
      // the classloader context!
      throw new AssertionError("hbase-default.xml not on classpath");
    }
    if (!"jar".equals(url.getProtocol()) &&
        !url.getPath().endsWith("target/classes/hbase-default.xml")) {
      throw new RuntimeException(
        "hbase-defaults.xml is being loaded from " + url + " rather than " +
        "the HBase JAR. This is dangerous since you may pick up defaults " +
        "from a previously installed version of HBase. Please remove " +
        "hbase-default.xml from your configuration directory.");
    }
  }

    


  public static Configuration addHbaseResources(Configuration conf) {
    checkDefaultsInJar(conf);

    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");
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
    for (Entry<String, String>e: that) {
      conf.set(e.getKey(), e.getValue());
    }
    return conf;
  }
}
