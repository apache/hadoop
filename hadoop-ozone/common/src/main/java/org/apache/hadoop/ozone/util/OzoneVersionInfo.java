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

package org.apache.hadoop.ozone.util;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hadoop.utils.HddsVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class returns build information about Hadoop components.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OzoneVersionInfo {
  private static final Logger LOG = LoggerFactory.getLogger(OzoneVersionInfo.class);

  private Properties info;

  protected OzoneVersionInfo(String component) {
    info = new Properties();
    String versionInfoFile = component + "-version-info.properties";
    InputStream is = null;
    try {
      is = ThreadUtil.getResourceAsStream(OzoneVersionInfo.class.getClassLoader(),
          versionInfoFile);
      info.load(is);
    } catch (IOException ex) {
      LoggerFactory.getLogger(getClass()).warn("Could not read '" +
          versionInfoFile + "', " + ex.toString(), ex);
    } finally {
      IOUtils.closeStream(is);
    }
  }

  protected String _getVersion() {
    return info.getProperty("version", "Unknown");
  }

  protected String _getRelease() {
    return info.getProperty("release", "Unknown");
  }

  protected String _getRevision() {
    return info.getProperty("revision", "Unknown");
  }

  protected String _getBranch() {
    return info.getProperty("branch", "Unknown");
  }

  protected String _getDate() {
    return info.getProperty("date", "Unknown");
  }

  protected String _getUser() {
    return info.getProperty("user", "Unknown");
  }

  protected String _getUrl() {
    return info.getProperty("url", "Unknown");
  }

  protected String _getSrcChecksum() {
    return info.getProperty("srcChecksum", "Unknown");
  }

  protected String _getBuildVersion(){
    return _getVersion() +
      " from " + _getRevision() +
      " by " + _getUser() +
      " source checksum " + _getSrcChecksum();
  }

  protected String _getProtocVersion() {
    return info.getProperty("protocVersion", "Unknown");
  }

  private static OzoneVersionInfo OZONE_VERSION_INFO = new OzoneVersionInfo("ozone");
  /**
   * Get the Ozone version.
   * @return the Ozone version string, eg. "0.6.3-dev"
   */
  public static String getVersion() {
    return OZONE_VERSION_INFO._getVersion();
  }

  /**
   * Get the Ozone release name.
   * @return the Ozone release string, eg. "Acadia"
   */
  public static String getRelease() {
    return OZONE_VERSION_INFO._getRelease();
  }

  /**
   * Get the Git commit hash of the repository when compiled.
   * @return the commit hash, eg. "18f64065d5db6208daf50b02c1b5ed4ee3ce547a"
   */
  public static String getRevision() {
    return OZONE_VERSION_INFO._getRevision();
  }

  /**
   * Get the branch on which this originated.
   * @return The branch name, e.g. "trunk" or "branches/branch-0.20"
   */
  public static String getBranch() {
    return OZONE_VERSION_INFO._getBranch();
  }

  /**
   * The date that Ozone was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return OZONE_VERSION_INFO._getDate();
  }

  /**
   * The user that compiled Ozone.
   * @return the username of the user
   */
  public static String getUser() {
    return OZONE_VERSION_INFO._getUser();
  }

  /**
   * Get the URL for the Ozone repository.
   * @return the URL of the Ozone repository
   */
  public static String getUrl() {
    return OZONE_VERSION_INFO._getUrl();
  }

  /**
   * Get the checksum of the source files from which Ozone was built.
   * @return the checksum of the source files
   */
  public static String getSrcChecksum() {
    return OZONE_VERSION_INFO._getSrcChecksum();
  }

  /**
   * Returns the buildVersion which includes version,
   * revision, user and date.
   * @return the buildVersion
   */
  public static String getBuildVersion(){
    return OZONE_VERSION_INFO._getBuildVersion();
  }

  /**
   * Returns the protoc version used for the build.
   * @return the protoc version
   */
  public static String getProtocVersion(){
    return OZONE_VERSION_INFO._getProtocVersion();
  }

  public static void main(String[] args) {
    System.out.println(
        "                  //////////////                 \n" +
        "               ////////////////////              \n" +
        "            ////////     ////////////////        \n" +
        "           //////      ////////////////          \n" +
        "          /////      ////////////////  /         \n" +
        "         /////            ////////   ///         \n" +
        "         ////           ////////    /////        \n" +
        "        /////         ////////////////           \n" +
        "        /////       ////////////////   //        \n" +
        "         ////     ///////////////   /////        \n" +
        "         /////  ///////////////     ////         \n" +
        "          /////       //////      /////          \n" +
        "           //////   //////       /////           \n" +
        "             ///////////     ////////            \n" +
        "               //////  ////////////              \n" +
        "               ///   //////////                  \n" +
        "              /    "+ getVersion() + "("+ getRelease() +")\n");
    System.out.println("Source code repository " + getUrl() + " -r " +
        getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("Compiled with protoc " + getProtocVersion());
    System.out.println("From source with checksum " + getSrcChecksum() + "\n");
    LOG.debug("This command was run using " +
        ClassUtil.findContainingJar(OzoneVersionInfo.class));
    HddsVersionInfo.main(args);
  }
}
