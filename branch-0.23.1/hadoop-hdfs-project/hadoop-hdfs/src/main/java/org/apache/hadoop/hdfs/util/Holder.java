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
package org.apache.hadoop.hdfs.util;

/**
 * A Holder is simply a wrapper around some other object. This is useful
 * in particular for storing immutable values like boxed Integers in a
 * collection without having to do the &quot;lookup&quot; of the value twice.
 */
public class Holder<T> {
  public T held;
  
  public Holder(T held) {
    this.held = held;
  }
  
  @Override
  public String toString() {
    return String.valueOf(held);
  }
}
