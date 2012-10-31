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

package org.apache.hadoop.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;

/**
 * Provides equivalent behavior to String.intern() to optimize performance, 
 * whereby does not consume memory in the permanent generation.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class StringInterner {
  
  /**
   * Retains a strong reference to each string instance it has interned.
   */
  private final static Interner<String> strongInterner;
  
  /**
   * Retains a weak reference to each string instance it has interned. 
   */
  private final static Interner<String> weakInterner;
  
  
  
  static {
    strongInterner = Interners.newStrongInterner();
    weakInterner = Interners.newWeakInterner();
  }
  
  /**
   * Interns and returns a reference to the representative instance 
   * for any of a collection of string instances that are equal to each other.
   * Retains strong reference to the instance, 
   * thus preventing it from being garbage-collected. 
   * 
   * @param sample string instance to be interned
   * @return strong reference to interned string instance
   */
  public static String strongIntern(String sample) {
    if (sample == null) {
      return null;
    }
    return strongInterner.intern(sample);
  }
  
  /**
   * Interns and returns a reference to the representative instance 
   * for any of a collection of string instances that are equal to each other.
   * Retains weak reference to the instance, 
   * and so does not prevent it from being garbage-collected.
   * 
   * @param sample string instance to be interned
   * @return weak reference to interned string instance
   */
  public static String weakIntern(String sample) {
    if (sample == null) {
      return null;
    }
    return weakInterner.intern(sample);
  }

}
