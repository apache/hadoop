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
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import static java.security.AccessController.*;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsPlugin;
import org.apache.hadoop.metrics2.filter.GlobFilter;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metrics configuration for MetricsSystemImpl
 */
class MetricsConfig extends SubsetConfiguration {
  static final Logger LOG = LoggerFactory.getLogger(MetricsConfig.class);

  static final String DEFAULT_FILE_NAME = "hadoop-metrics2.properties";
  static final String PREFIX_DEFAULT = "*.";

  static final String PERIOD_KEY = "period";
  static final int PERIOD_DEFAULT = 10; // seconds

  // For testing, this will have the priority.
  static final String PERIOD_MILLIS_KEY = "periodMillis";

  static final String QUEUE_CAPACITY_KEY = "queue.capacity";
  static final int QUEUE_CAPACITY_DEFAULT = 1;

  static final String RETRY_DELAY_KEY = "retry.delay";
  static final int RETRY_DELAY_DEFAULT = 10;  // seconds
  static final String RETRY_BACKOFF_KEY = "retry.backoff";
  static final int RETRY_BACKOFF_DEFAULT = 2; // back off factor
  static final String RETRY_COUNT_KEY = "retry.count";
  static final int RETRY_COUNT_DEFAULT = 1;

  static final String JMX_CACHE_TTL_KEY = "jmx.cache.ttl";
  static final String START_MBEANS_KEY = "source.start_mbeans";
  static final String PLUGIN_URLS_KEY = "plugin.urls";

  static final String CONTEXT_KEY = "context";
  static final String NAME_KEY = "name";
  static final String DESC_KEY = "description";
  static final String SOURCE_KEY = "source";
  static final String SINK_KEY = "sink";
  static final String METRIC_FILTER_KEY = "metric.filter";
  static final String RECORD_FILTER_KEY = "record.filter";
  static final String SOURCE_FILTER_KEY = "source.filter";

  static final Pattern INSTANCE_REGEX = Pattern.compile("([^.*]+)\\..+");
  static final Splitter SPLITTER = Splitter.on(',').trimResults();
  private ClassLoader pluginLoader;

  MetricsConfig(Configuration c, String prefix) {
    super(c, StringUtils.toLowerCase(prefix), ".");
  }

  static MetricsConfig create(String prefix) {
    return loadFirst(prefix, "hadoop-metrics2-" +
        StringUtils.toLowerCase(prefix) + ".properties", DEFAULT_FILE_NAME);
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
        PropertiesConfiguration pcf = new PropertiesConfiguration();
        FileHandler fh = new FileHandler(pcf);
        fh.setFileName(fname);
        fh.load();
        Configuration cf = pcf.interpolatedConfiguration();
        LOG.info("Loaded properties from {}", fname);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Properties: {}", toString(cf));
        }
        MetricsConfig mc = new MetricsConfig(cf, prefix);
        LOG.debug("Metrics Config: {}", mc);
        return mc;
      } catch (ConfigurationException e) {
        // Commons Configuration defines the message text when file not found
        if (e.getMessage().startsWith("Could not locate")) {
          LOG.debug("Could not locate file {}", fname, e);
          continue;
        }
        throw new MetricsConfigException(e);
      }
    }
    LOG.warn("Cannot locate configuration: tried " +
             Joiner.on(",").join(fileNames));
    // default to an empty configuration
    return new MetricsConfig(new PropertiesConfiguration(), prefix);
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
    Map<String, MetricsConfig> map = Maps.newHashMap();
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
      @Override
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
  public Object getPropertyInternal(String key) {
    Object value = super.getPropertyInternal(key);
    if (value == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("poking parent '" + getParent().getClass().getSimpleName() +
                  "' for key: " + key);
      }
      return getParent().getProperty(key.startsWith(PREFIX_DEFAULT) ? key
                                     : PREFIX_DEFAULT + key);
    }
    LOG.debug("Returning '{}' for key: {}", value, key);
    return value;
  }

  <T extends MetricsPlugin> T getPlugin(String name) {
    String clsName = getClassName(name);
    if (clsName == null) {
      return null;
    }
    try {
      Class<?> cls = Class.forName(clsName, true, getPluginLoader());
      @SuppressWarnings("unchecked")
      T plugin = (T) cls.newInstance();
      plugin.init(name.isEmpty() ? this : subset(name));
      return plugin;
    } catch (Exception e) {
      throw new MetricsConfigException("Error creating plugin: "+ clsName, e);
    }
  }

  String getClassName(String prefix) {
    String classKey = prefix.isEmpty() ? "class" : prefix.concat(".class");
    String clsName = getString(classKey);
    LOG.debug("Class name for prefix {} is {}", prefix, clsName);
    if (clsName == null || clsName.isEmpty()) {
      return null;
    }
    return clsName;
  }

  ClassLoader getPluginLoader() {
    if (pluginLoader != null) {
      return pluginLoader;
    }
    final ClassLoader defaultLoader = getClass().getClassLoader();
    Object purls = super.getProperty(PLUGIN_URLS_KEY);
    if (purls == null) {
      return defaultLoader;
    }
    Iterable<String> jars = SPLITTER.split((String) purls);
    int len = Iterables.size(jars);
    if (len > 0) {
      final URL[] urls = new URL[len];
      try {
        int i = 0;
        for (String jar : jars) {
          LOG.debug("Parsing URL for {}", jar);
          urls[i++] = new URL(jar);
        }
      } catch (Exception e) {
        throw new MetricsConfigException(e);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Using plugin jars: {}", Iterables.toString(jars));
      }
      pluginLoader = doPrivileged(new PrivilegedAction<ClassLoader>() {
        @Override public ClassLoader run() {
          return new URLClassLoader(urls, defaultLoader);
        }
      });
      return pluginLoader;
    }
    if (parent instanceof MetricsConfig) {
      return ((MetricsConfig) parent).getPluginLoader();
    }
    return defaultLoader;
  }

  MetricsFilter getFilter(String prefix) {
    // don't create filter instances without out options
    MetricsConfig conf = subset(prefix);
    if (conf.isEmpty()) {
      return null;
    }
    MetricsFilter filter = getPlugin(prefix);
    if (filter != null) {
      return filter;
    }
    // glob filter is assumed if pattern is specified but class is not.
    filter = new GlobFilter();
    filter.init(conf);
    return filter;
  }

  @Override
  public String toString() {
    return toString(this);
  }

  static String toString(Configuration c) {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try {
      PrintWriter pw = new PrintWriter(buffer, false);
      PropertiesConfiguration tmp = new PropertiesConfiguration();
      tmp.copy(c);
      tmp.write(pw);
      return buffer.toString("UTF-8");
    } catch (Exception e) {
      throw new MetricsConfigException(e);
    }
  }
}
