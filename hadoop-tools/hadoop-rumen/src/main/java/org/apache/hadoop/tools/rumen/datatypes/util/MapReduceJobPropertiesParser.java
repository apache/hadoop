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
package org.apache.hadoop.tools.rumen.datatypes.util;

import java.lang.reflect.Field;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.tools.rumen.datatypes.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * A default parser for MapReduce job configuration properties.
 * MapReduce job configuration properties are represented as key-value pairs. 
 * Each key represents a configuration knob which controls or affects the 
 * behavior of a MapReduce job or a job's task. The value associated with the 
 * configuration key represents its value. Some of the keys are deprecated. As a
 * result of deprecation some keys change or are preferred over other keys, 
 * across versions. {@link MapReduceJobPropertiesParser} is a utility class that
 * parses MapReduce job configuration properties and converts the value into a 
 * well defined {@link DataType}. Users can use the 
 * {@link MapReduceJobPropertiesParser#parseJobProperty(String, String)} API to 
 * process job configuration parameters. This API will parse a job property 
 * represented as a key-value pair and return the value wrapped inside a 
 * {@link DataType}. Callers can then use the returned {@link DataType} for 
 * further processing.
 * 
 * {@link MapReduceJobPropertiesParser} thrives on the key name to decide which
 * {@link DataType} to wrap the value with. Values for keys representing 
 * job-name, queue-name, user-name etc are wrapped inside {@link JobName}, 
 * {@link QueueName}, {@link UserName} etc respectively. Keys ending with *dir* 
 * are considered as a directory and hence gets be wrapped inside 
 * {@link FileName}. Similarly key ending with *codec*, *log*, *class* etc are
 * also handled accordingly. Values representing basic java data-types like 
 * integer, float, double, boolean etc are wrapped inside 
 * {@link DefaultDataType}. If the key represents some jvm-level settings then 
 * only standard settings are extracted and gets wrapped inside 
 * {@link DefaultDataType}. Currently only '-Xmx' and '-Xms' settings are 
 * considered while the rest are ignored.
 * 
 * Note that the {@link MapReduceJobPropertiesParser#parseJobProperty(String, 
 * String)} API maps the keys to a configuration parameter listed in 
 * {@link MRJobConfig}. This not only filters non-framework specific keys thus 
 * ignoring user-specific and hard-to-parse keys but also provides a consistent 
 * view for all possible inputs. So if users invoke the 
 * {@link MapReduceJobPropertiesParser#parseJobProperty(String, String)} API
 * with either <"mapreduce.job.user.name", "bob"> or <"user.name", "bob">, then 
 * the result would be a {@link UserName} {@link DataType} wrapping the user-name "bob".
 */
@SuppressWarnings("deprecation")
public class MapReduceJobPropertiesParser implements JobPropertyParser {
  private Field[] mrFields = MRJobConfig.class.getFields();
  private DecimalFormat format = new DecimalFormat();
  private JobConf configuration = new JobConf(false);
  private static final Pattern MAX_HEAP_PATTERN = 
    Pattern.compile("-Xmx[0-9]+[kKmMgGtT]?+");
  private static final Pattern MIN_HEAP_PATTERN = 
    Pattern.compile("-Xms[0-9]+[kKmMgGtT]?+");
  
  // turn off the warning w.r.t deprecated mapreduce keys
  static {
    Logger.getLogger(Configuration.class).setLevel(Level.OFF);
  }
    
  // Accepts a key if there is a corresponding key in the current mapreduce
  // configuration
  private boolean accept(String key) {
    return getLatestKeyName(key) != null;
  }
  
  // Finds a corresponding key for the specified key in the current mapreduce
  // setup.
  // Note that this API uses a cached copy of the Configuration object. This is
  // purely for performance reasons.
  private String getLatestKeyName(String key) {
    // set the specified key
    configuration.set(key, key);
    try {
      // check if keys in MRConfig maps to the specified key.
      for (Field f : mrFields) {
        String mrKey = f.get(f.getName()).toString();
        if (configuration.get(mrKey) != null) {
          return mrKey;
        }
      }
      
      // unset the key
      return null;
    } catch (IllegalAccessException iae) {
      throw new RuntimeException(iae);
    } finally {
      // clean up!
      configuration.clear();
    }
  }
  
  @Override
  public DataType<?> parseJobProperty(String key, String value) {
    if (accept(key)) {
      return fromString(key, value);
    }
    
    return null;
  }
  
  /**
   * Extracts the -Xmx heap option from the specified string.
   */
  public static void extractMaxHeapOpts(String javaOptions, 
                                        List<String> heapOpts, 
                                        List<String> others) {
    for (String opt : javaOptions.split(" ")) {
      Matcher matcher = MAX_HEAP_PATTERN.matcher(opt);
      if (matcher.find()) {
        heapOpts.add(opt);
      } else {
        others.add(opt);
      }
    }
  }
  
  /**
   * Extracts the -Xms heap option from the specified string.
   */
  public static void extractMinHeapOpts(String javaOptions,  
      List<String> heapOpts,  List<String> others) {
    for (String opt : javaOptions.split(" ")) {
      Matcher matcher = MIN_HEAP_PATTERN.matcher(opt);
      if (matcher.find()) {
        heapOpts.add(opt);
      } else {
        others.add(opt);
      }
    }
  }
  
  // Maps the value of the specified key.
  private DataType<?> fromString(String key, String value) {
    if (value != null) {
      // check known configs
      //  job-name
      String latestKey = getLatestKeyName(key);
      
      if (MRJobConfig.JOB_NAME.equals(latestKey)) {
        return new JobName(value);
      }
      // user-name
      if (MRJobConfig.USER_NAME.equals(latestKey)) {
        return new UserName(value);
      }
      // queue-name
      if (MRJobConfig.QUEUE_NAME.equals(latestKey)) {
        return new QueueName(value);
      }
      if (MRJobConfig.MAP_JAVA_OPTS.equals(latestKey) 
          || MRJobConfig.REDUCE_JAVA_OPTS.equals(latestKey)) {
        List<String> heapOptions = new ArrayList<String>();
        extractMaxHeapOpts(value, heapOptions, new ArrayList<String>());
        extractMinHeapOpts(value, heapOptions, new ArrayList<String>());
        return new DefaultDataType(StringUtils.join(heapOptions, ' '));
      }
      
      //TODO compression?
      //TODO Other job configs like FileOutputFormat/FileInputFormat etc

      // check if the config parameter represents a number
      try {
        format.parse(value);
        return new DefaultDataType(value);
      } catch (ParseException pe) {}

      // check if the config parameters represents a boolean 
      // avoiding exceptions
      if ("true".equals(value) || "false".equals(value)) {
        Boolean.parseBoolean(value);
        return new DefaultDataType(value);
      }

      // check if the config parameter represents a class
      if (latestKey.endsWith(".class") || latestKey.endsWith(".codec")) {
        return new ClassName(value);
      }

      // handle distributed cache sizes and timestamps
      if (latestKey.endsWith("sizes") 
          || latestKey.endsWith(".timestamps")) {
        new DefaultDataType(value);
      }
      
      // check if the config parameter represents a file-system path
      //TODO: Make this concrete .location .path .dir .jar?
      if (latestKey.endsWith(".dir") || latestKey.endsWith(".location") 
          || latestKey.endsWith(".jar") || latestKey.endsWith(".path") 
          || latestKey.endsWith(".logfile") || latestKey.endsWith(".file")
          || latestKey.endsWith(".files") || latestKey.endsWith(".archives")) {
        try {
          return new FileName(value);
        } catch (Exception ioe) {}
      }
    }

    return null;
  }
}