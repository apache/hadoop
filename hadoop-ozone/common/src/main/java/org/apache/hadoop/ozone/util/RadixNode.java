/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.util;

import java.util.HashMap;

/**
 * Wrapper class for Radix tree node representing Ozone prefix path segment
 * separated by "/".
 */
public class RadixNode<T> {

  public RadixNode(String name) {
    this.name = name;
    this.children = new HashMap<>();
  }

  public String getName() {
    return name;
  }

  public boolean hasChildren() {
    return children.isEmpty();
  }

  public HashMap<String, RadixNode> getChildren() {
    return children;
  }

  public void setValue(T v) {
    this.value = v;
  }

  public T getValue() {
    return value;
  }

  private HashMap<String, RadixNode> children;

  private String name;

  // TODO: k/v pairs for more metadata as needed
  private T value;
}
