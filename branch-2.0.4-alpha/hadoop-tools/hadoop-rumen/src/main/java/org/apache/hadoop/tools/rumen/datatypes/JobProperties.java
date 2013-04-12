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
package org.apache.hadoop.tools.rumen.datatypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.rumen.datatypes.util.JobPropertyParser;
import org.apache.hadoop.tools.rumen.datatypes.util.MapReduceJobPropertiesParser;
import org.apache.hadoop.tools.rumen.state.StatePool;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This represents the job configuration properties.
 */
public class JobProperties implements AnonymizableDataType<Properties> {
  public static final String PARSERS_CONFIG_KEY = 
    "rumen.datatypes.jobproperties.parsers";
  private final Properties jobProperties;
  
  public JobProperties() {
    this(new Properties());
  }
  
  public JobProperties(Properties properties) {
    this.jobProperties = properties;
  }
  
  public Properties getValue() {
    return jobProperties;
  }
  
  @Override
  public Properties getAnonymizedValue(StatePool statePool, 
                                       Configuration conf) {
    Properties filteredProperties = null;
    List<JobPropertyParser> pList = new ArrayList<JobPropertyParser>(1);
    // load the parsers
    String config = conf.get(PARSERS_CONFIG_KEY);
    if (config != null) {
      @SuppressWarnings("unchecked")
      Class<JobPropertyParser>[] parsers = 
        (Class[])conf.getClasses(PARSERS_CONFIG_KEY);
      for (Class<JobPropertyParser> c : parsers) {
        JobPropertyParser parser = ReflectionUtils.newInstance(c, conf);
        pList.add(parser);
      }
    } else {
      // add the default MapReduce filter
      JobPropertyParser parser = new MapReduceJobPropertiesParser();
      pList.add(parser);
    }
    
    // filter out the desired config key-value pairs
    if (jobProperties != null) {
      filteredProperties = new Properties();
      // define a configuration object and load it with original job properties
      for (Map.Entry<Object, Object> entry : jobProperties.entrySet()) {
        //TODO Check for null key/value?
        String key = entry.getKey().toString();
        String value = entry.getValue().toString(); 
        
        // find a parser for this key
        for (JobPropertyParser p : pList) {
          DataType<?> pValue = p.parseJobProperty(key, value);
          if (pValue != null) {
            filteredProperties.put(key, pValue);
            break;
          }
        }
      }
    }
    return filteredProperties;
  }
}