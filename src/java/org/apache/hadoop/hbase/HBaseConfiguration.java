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

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

/**
 * Adds HBase configuration files to a Configuration
 */
public class HBaseConfiguration extends Configuration {
  /** constructor */
  public HBaseConfiguration() {
    super();
    addHbaseResources();
  }
  
  /**
   * Create a clone of passed configuration.
   * @param c Configuration to clone.
   */
  public HBaseConfiguration(final Configuration c) {
    this();
    for (Entry<String, String>e: c) {
      set(e.getKey(), e.getValue());
    }
  }
  
  private void addHbaseResources() {
    addResource("hbase-default.xml");
    addResource("hbase-site.xml");
  }
  
  /**
   * Returns the hash code value for this HBaseConfiguration. The hash code of a
   * HBaseConfiguration is defined by the sum of the hash codes of its entries.
   * 
   * @see Configuration#iterator() How the entries are obtained.
   */
  @Override
  public int hashCode() {
    int hash = 0;

    Iterator<Entry<String, String>> propertyIterator = this.iterator();
    while (propertyIterator.hasNext()) {
      hash ^= propertyIterator.next().hashCode();
    }
    return hash;
  }
  
}
