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

package org.apache.slider.core.registry.docstore;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * JSON-serializable description of a published key-val configuration.
 *
 * The values themselves are not serialized in the external view; they have to be served up by the far end
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class PublishedExports {

  public String description;
  public long updated;
  public String updatedTime;
  public Map<String, Set<ExportEntry>> entries = new HashMap<>();

  public PublishedExports() {
  }

  /**
   * build an empty published configuration
   *
   * @param description configuration description
   */
  public PublishedExports(String description) {
    this.description = description;
  }

  /**
   * Build a configuration from the entries
   *
   * @param description configuration description
   * @param entries     entries to put
   */
  public PublishedExports(String description,
                          Iterable<Entry<String, Set<ExportEntry>>> entries) {
    this.description = description;
    putValues(entries);
  }

  /**
   * Is the configuration empty. This means either that it has not been given any values,
   * or it is stripped down copy
   * set down over the wire.
   *
   * @return true if it is empty
   */
  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public long getUpdated() {
    return updated;
  }

  public void setUpdated(long updated) {
    this.updated = updated;
    this.updatedTime = new Date(updated).toString();
  }


  public Map<String, Set<ExportEntry>> sortedEntries() {
    Map<String, Set<ExportEntry>> sortedEntries = new TreeMap<>();
    sortedEntries.putAll(entries);
    return sortedEntries;
  }

  /**
   * Set the values from an iterable (this includes a Hadoop Configuration and Java properties
   * object). Any existing value set is discarded
   *
   * @param values values to put
   */
  public void putValues(Iterable<Map.Entry<String, Set<ExportEntry>>> values) {
    this.entries = new HashMap<>();
    for (Map.Entry<String, Set<ExportEntry>> entry : values) {
      this.entries.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Return the values as json string
   *
   * @return the JSON form
   *
   * @throws IOException mapping problems
   */
  public String asJson() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    String json = mapper.writeValueAsString(entries);
    return json;
  }

  /**
   * This makes a copy without the nested content -so is suitable for returning as part of the list of a parent's
   * values
   *
   * @return the copy
   */
  public PublishedExports shallowCopy() {
    PublishedExports that = new PublishedExports();
    that.description = this.description;
    that.updated = this.updated;
    that.updatedTime = this.updatedTime;
    return that;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("PublishedConfiguration{");
    sb.append("description='").append(description).append('\'');
    sb.append(" entries = ").append(entries.size());
    sb.append('}');
    return sb.toString();
  }
}
