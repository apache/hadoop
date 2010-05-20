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
package org.apache.hadoop.hbase.util;

import java.util.HashSet;
import java.util.Set;

/**
 * Certain JVM versions are known to be unstable with HBase. This
 * class has a utility function to determine whether the current JVM
 * is known to be unstable.
 */
public abstract class JvmVersion {
  private static Set<String> BAD_JVM_VERSIONS = new HashSet<String>();
  static {
    BAD_JVM_VERSIONS.add("1.6.0_18");
  }

  /**
   * Return true if the current JVM is known to be unstable.
   */
  public static boolean isBadJvmVersion() {
    String version = System.getProperty("java.version");
    return version != null && BAD_JVM_VERSIONS.contains(version);
  }
}
