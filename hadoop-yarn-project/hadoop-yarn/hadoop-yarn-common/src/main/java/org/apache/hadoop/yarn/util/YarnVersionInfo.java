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

package org.apache.hadoop.yarn.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This class finds the package info for Yarn.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class YarnVersionInfo extends VersionInfo {
  private static final Log LOG = LogFactory.getLog(YarnVersionInfo.class);

  private static YarnVersionInfo YARN_VERSION_INFO = new YarnVersionInfo();

  protected YarnVersionInfo() {
    super("yarn");
  }
  /**
   * Get the YARN version.
   * @return the YARN version string, eg. "0.6.3-dev"
   */
  public static String getVersion() {
    return YARN_VERSION_INFO._getVersion();
  }
  
  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getRevision() {
    return YARN_VERSION_INFO._getRevision();
  }

  /**
   * Get the branch on which this originated.
   * @return The branch name, e.g. "trunk" or "branches/branch-0.20"
   */
  public static String getBranch() {
    return YARN_VERSION_INFO._getBranch();
  }

  /**
   * The date that YARN was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return YARN_VERSION_INFO._getDate();
  }
  
  /**
   * The user that compiled Yarn.
   * @return the username of the user
   */
  public static String getUser() {
    return YARN_VERSION_INFO._getUser();
  }
  
  /**
   * Get the subversion URL for the root YARN directory.
   */
  public static String getUrl() {
    return YARN_VERSION_INFO._getUrl();
  }

  /**
   * Get the checksum of the source files from which YARN was
   * built.
   **/
  public static String getSrcChecksum() {
    return YARN_VERSION_INFO._getSrcChecksum();
  }

  /**
   * Returns the buildVersion which includes version, 
   * revision, user and date. 
   */
  public static String getBuildVersion(){
    return YARN_VERSION_INFO._getBuildVersion();
  }
  
  public static void main(String[] args) {
    LOG.debug("version: "+ getVersion());
    System.out.println("YARN " + getVersion());
    System.out.println("Subversion " + getUrl() + " -r " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("From source with checksum " + getSrcChecksum());
  }
}
