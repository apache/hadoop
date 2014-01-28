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

/**
 * A wrapper class to maven's ComparableVersion class, to comply
 * with maven's version name string convention 
 */
@InterfaceAudience.Private
public abstract class VersionUtil {
  /**
   * Compares two version name strings using maven's ComparableVersion class.
   *
   * @param version1
   *          the first version to compare
   * @param version2
   *          the second version to compare
   * @return a negative integer if version1 precedes version2, a positive
   *         integer if version2 precedes version1, and 0 if and only if the two
   *         versions are equal.
   */
  public static int compareVersions(String version1, String version2) {
    ComparableVersion v1 = new ComparableVersion(version1);
    ComparableVersion v2 = new ComparableVersion(version2);
    return v1.compareTo(v2);
  }
}
