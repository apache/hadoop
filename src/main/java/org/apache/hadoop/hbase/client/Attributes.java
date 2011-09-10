/*
 * Copyright 2011 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import java.util.Map;

public interface Attributes {
  /**
   * Sets an attribute.
   * In case value = null attribute is removed from the attributes map.
   * @param name attribute name
   * @param value attribute value
   */
  public void setAttribute(String name, byte[] value);

  /**
   * Gets an attribute
   * @param name attribute name
   * @return attribute value if attribute is set, <tt>null</tt> otherwise
   */
  public byte[] getAttribute(String name);

  /**
   * Gets all attributes
   * @return unmodifiable map of all attributes
   */
  public Map<String, byte[]> getAttributesMap();
}
