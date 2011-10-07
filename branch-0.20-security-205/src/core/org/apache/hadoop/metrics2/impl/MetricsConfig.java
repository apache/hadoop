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

package org.apache.hadoop.metrics2.impl;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsPlugin;

import org.apache.hadoop.util.StringUtils;

class MetricsConfig extends SubsetConfiguration {

  static final Log LOG = LogFactory.getLog(MetricsConfig.class);

  static final String DEFAULT_FILE_NAME = "hadoop-metrics2.properties";
  static final String PREFIX_DEFAULT = "*.";

  static final String PERIOD_KEY = "period";
  static final int PERIOD_DEFAULT = 10; // seconds

  static final String QUEUE_CAPACITY_KEY = "queue.capacity";
  static final int QUEUE_CAPACITY_DEFAULT = 1;

  static final String RETRY_DELAY_KEY = "retry.delay";
  static final int RETRY_DELAY_DEFAULT = 10;  // seconds
  static final String RETRY_BACKOFF_KEY = "retry.backoff";
  static final int RETRY_BACKOFF_DEFAULT = 2; // back off factor
  static final String RETRY_COUNT_KEY = "retry.count";
  static final int RETRY_COUNT_DEFAULT = 1;

  static final String JMX_CACHE_TTL_KEY = "jmx.cache.ttl";
  static final int JMX_CACHE_TTL_DEFAULT = 10000; // millis

  static final String CONTEXT_KEY = "context";
  static final String NAME_KEY = "name";
  static final String DESC_KEY = "description";
  static final String SOURCE_KEY = "source";
  static final String SINK_KEY = "sink";
  static final String METRIC_FILTER_KEY = "metric.filter";
  static final String RECORD_FILTER_KEY = "record.filter";
  static final String SOURCE_FILTER_KEY = "source.filter";

  static final Pattern INSTANCE_REGEX = Pattern.compile("([^.*]+)\\..+");

  MetricsConfig(Configuration c, String prefix) {
    super(c, prefix.toLowerCase(Locale.US), ".");
  }

  static MetricsConfig create(String prefix) {
    return loadFirst(prefix, "hadoop-metrics2-"+ prefix.toLowerCase(Locale.US)
                     +".properties", DEFAULT_FILE_NAME);
  }

  static MetricsConfig create(String prefix, String... fileNames) {
    return loadFirst(prefix, fileNames);
  }

  /**
   * Load configuration from a list of files until the first successful load
   * @param conf  the configuration object
   * @param files the list of filenames to try
   * @return  the configuration object
   */
  static MetricsConfig loadFirst(String prefix, String... fileNames) {
    for (String fname : fileNames) {
      try {
        Configuration cf = new PropertiesConfiguration(fname)
            .interpolatedConfiguration();
        LOG.info("loaded properties from "+ fname);
        return new MetricsConfig(cf, prefix);
      }
      catch (ConfigurationException e) {
        if (e.getMessage().startsWith("Cannot locate configuration")) {
          continue;
        }
        throw new MetricsConfigException(e);
      }
    }
    throw new MetricsConfigException("Cannot locate configuration: tried "+
        StringUtils.join(", ", fileNames));
  }

  @Override
  public MetricsConfig subset(String prefix) {
    return new MetricsConfig(this, prefix);
  }

  /**
   * Return sub configs for instance specified in the config.
   * Assuming format specified as follows:<pre>
   * [type].[instance].[option] = [value]</pre>
   * Note, '*' is a special default instance, which is excluded in the result.
   * @param type  of the instance
   * @return  a map with [instance] as key and config object as value
   */
  Map<String, MetricsConfig> getInstanceConfigs(String type) {
    HashMap<String, MetricsConfig> map = new HashMap<String, MetricsConfig>();
    MetricsConfig sub = subset(type);

    for (String key : sub.keys()) {
      Matcher matcher = INSTANCE_REGEX.matcher(key);
      if (matcher.matches()) {
        String instance = matcher.group(1);
        if (!map.containsKey(instance)) {
          map.put(instance, sub.subset(instance));
        }
      }
    }
    return map;
  }

  Iterable<String> keys() {
    return new Iterable<String>() {
      @SuppressWarnings("unchecked")
      public Iterator<String> iterator() {
        return (Iterator<String>) getKeys();
      }
    };
  }

  /**
   * Will poke parents for defaults
   * @param key to lookup
   * @return  the value or null
   */
  @Override
  public Object getProperty(String key) {
    Object value = super.getProperty(key);
    if (value == null) {
      LOG.debug("poking parent "+ getParent().getClass().getSimpleName() +
                " for "+ key);
      return getParent().getProperty(key.startsWith(PREFIX_DEFAULT) ? key
                                     : PREFIX_DEFAULT + key);
    }
    return value;
  }

  <T extends MetricsPlugin> T getPlugin(String name) {
    String classKey = name.isEmpty() ? "class" : name +".class";
    String pluginClassName = getString(classKey);
    if (pluginClassName == null || pluginClassName.isEmpty()) {
      return null;
    }
    try {
      Class<?> pluginClass = Class.forName(pluginClassName);
      @SuppressWarnings("unchecked")
      T plugin = (T) pluginClass.newInstance();
      plugin.init(name.isEmpty() ? this : subset(name));
      return plugin;
    }
    catch (Exception e) {
      throw new MetricsConfigException("Error creating plugin: "+
                                       pluginClassName, e);
    }
  }

  MetricsFilter getFilter(String prefix) {
    // don't create filter instances without out options
    if (subset(prefix).isEmpty()) return null;
    return (MetricsFilter) getPlugin(prefix);
  }

  @Override
  public String toString() {
    return toString(this);
  }

  String toString(Configuration c) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(buffer);
    PropertiesConfiguration tmp = new PropertiesConfiguration();
    tmp.copy(c);
    try { tmp.save(ps); }
    catch (Exception e) {
      throw new MetricsConfigException(e);
    }
    return buffer.toString();
  }

}
