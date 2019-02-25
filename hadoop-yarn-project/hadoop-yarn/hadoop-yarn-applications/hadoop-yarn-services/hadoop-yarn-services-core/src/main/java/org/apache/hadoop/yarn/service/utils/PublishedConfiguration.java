/*
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

package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.exceptions.BadConfigException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * JSON-serializable description of a published key-val configuration.
 * 
 * The values themselves are not serialized in the external view; they have
 * to be served up by the far end
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class PublishedConfiguration {

  public String description;
  public long updated;
  
  public String updatedTime;

  public Map<String, String> entries = new HashMap<>();

  public PublishedConfiguration() {
  }

  /**
   * build an empty published configuration 
   * @param description configuration description
   */
  public PublishedConfiguration(String description) {
    this.description = description;
  }

  /**
   * Build a configuration from the entries
   * @param description configuration description
   * @param entries entries to put
   */
  public PublishedConfiguration(String description,
      Iterable<Map.Entry<String, String>> entries) {
    this.description = description;
    putValues(entries);
  }

  /**
   * Build a published configuration, using the keys from keysource,
   * but resolving the values from the value source, via Configuration.get()
   * @param description configuration description
   * @param keysource source of keys
   * @param valuesource source of values
   */
  public PublishedConfiguration(String description,
      Iterable<Map.Entry<String, String>> keysource,
      Configuration valuesource) {
    this.description = description;
    putValues(ConfigHelper.resolveConfiguration(keysource, valuesource));
  }

  
  /**
   * Is the configuration empty. This means either that it has not
   * been given any values, or it is stripped down copy set down over the
   * wire.
   * @return true if it is empty
   */
  public boolean isEmpty() {
    return entries.isEmpty();
  }


  public void setUpdated(long updated) {
    this.updated = updated;
    this.updatedTime = new Date(updated).toString();
  }

  public long getUpdated() {
    return updated;
  }

  /**
   * Set the values from an iterable (this includes a Hadoop Configuration
   * and Java properties object).
   * Any existing value set is discarded
   * @param entries entries to put
   */
  public void putValues(Iterable<Map.Entry<String, String>> entries) {
    this.entries = new HashMap<String, String>();
    for (Map.Entry<String, String> entry : entries) {
      this.entries.put(entry.getKey(), entry.getValue());
    }
    
  }

  /**
   * Convert to Hadoop XML
   * @return the configuration as a Hadoop Configuratin
   */
  public Configuration asConfiguration() {
    Configuration conf = new Configuration(false);
    try {
      ConfigHelper.addConfigMap(conf, entries, "");
    } catch (BadConfigException e) {
      // triggered on a null value; switch to a runtime (and discard the stack)
      throw new RuntimeException(e.toString());
    }
    return conf;
  }
  
  public String asConfigurationXML() throws IOException {
    return ConfigHelper.toXml(asConfiguration());
  }

  /**
   * Convert values to properties
   * @return a property file
   */
  public Properties asProperties() {
    Properties props = new Properties();
    props.putAll(entries);
    return props;
  }

  /**
   * Return the values as json string
   * @return the JSON representation
   * @throws IOException marshalling failure
   */
  public String asJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    String json = mapper.writeValueAsString(entries);
    return json;
  }


  /**
   * This makes a copy without the nested content -so is suitable
   * for returning as part of the list of a parent's values
   * @return the copy
   */
  public PublishedConfiguration shallowCopy() {
    PublishedConfiguration that = new PublishedConfiguration();
    that.description = this.description;
    that.updated = this.updated;
    that.updatedTime = this.updatedTime;
    return that;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("PublishedConfiguration{");
    sb.append("description='").append(description).append('\'')
        .append(" entries = ").append(entries.size())
        .append('}');
    return sb.toString();
  }

}
