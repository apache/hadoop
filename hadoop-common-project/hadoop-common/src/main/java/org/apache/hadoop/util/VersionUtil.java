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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.collect.ComparisonChain;

@InterfaceAudience.Private
public abstract class VersionUtil {
  
  private static final Pattern COMPONENT_GROUPS = Pattern.compile("(\\d+)|(\\D+)");

  /**
   * Suffix added by maven for nightly builds and other snapshot releases.
   * These releases are considered to precede the non-SNAPSHOT version
   * with the same version number.
   */
  private static final String SNAPSHOT_SUFFIX = "-SNAPSHOT";

  /**
   * This function splits the two versions on &quot;.&quot; and performs a
   * naturally-ordered comparison of the resulting components. For example, the
   * version string "0.3" is considered to precede "0.20", despite the fact that
   * lexical comparison would consider "0.20" to precede "0.3". This method of
   * comparison is similar to the method used by package versioning systems like
   * deb and RPM.
   * 
   * Version components are compared numerically whenever possible, however a
   * version component can contain non-numeric characters. When a non-numeric
   * group of characters is found in a version component, this group is compared
   * with the similarly-indexed group in the other version component. If the
   * other group is numeric, then the numeric group is considered to precede the
   * non-numeric group. If both groups are non-numeric, then a lexical
   * comparison is performed.
   * 
   * If two versions have a different number of components, then only the lower
   * number of components are compared. If those components are identical
   * between the two versions, then the version with fewer components is
   * considered to precede the version with more components.
   * 
   * In addition to the above rules, there is one special case: maven SNAPSHOT
   * releases are considered to precede a non-SNAPSHOT release with an
   * otherwise identical version number. For example, 2.0-SNAPSHOT precedes
   * 2.0.
   * 
   * This function returns a negative integer if version1 precedes version2, a
   * positive integer if version2 precedes version1, and 0 if and only if the
   * two versions' components are identical in value and cardinality.
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
    boolean isSnapshot1 = version1.endsWith(SNAPSHOT_SUFFIX);
    boolean isSnapshot2 = version2.endsWith(SNAPSHOT_SUFFIX);
    version1 = stripSnapshotSuffix(version1);
    version2 = stripSnapshotSuffix(version2);
    
    String[] version1Parts = version1.split("\\.");
    String[] version2Parts = version2.split("\\.");
    
    for (int i = 0; i < version1Parts.length && i < version2Parts.length; i++) {
      String component1 = version1Parts[i];
      String component2 = version2Parts[i];
      if (!component1.equals(component2)) {
        Matcher matcher1 = COMPONENT_GROUPS.matcher(component1);
        Matcher matcher2 = COMPONENT_GROUPS.matcher(component2);
        
        while (matcher1.find() && matcher2.find()) {
          String group1 = matcher1.group();
          String group2 = matcher2.group();
          if (!group1.equals(group2)) {
            if (isNumeric(group1) && isNumeric(group2)) {
              return Integer.parseInt(group1) - Integer.parseInt(group2);
            } else if (!isNumeric(group1) && !isNumeric(group2)) {
              return group1.compareTo(group2);
            } else {
              return isNumeric(group1) ? -1 : 1;
            }
          }
        }
        return component1.length() - component2.length();
      }
    }
    
    return ComparisonChain.start()
      .compare(version1Parts.length, version2Parts.length)
      .compare(isSnapshot2, isSnapshot1)
      .result();
  }
  
  private static String stripSnapshotSuffix(String version) {
    if (version.endsWith(SNAPSHOT_SUFFIX)) {
      return version.substring(0, version.length() - SNAPSHOT_SUFFIX.length());
    } else {
      return version;
    }
  }

  private static boolean isNumeric(String s) {
    try {
      Integer.parseInt(s);
      return true;
    } catch (NumberFormatException nfe) {
      return false;
    }
  }
}
