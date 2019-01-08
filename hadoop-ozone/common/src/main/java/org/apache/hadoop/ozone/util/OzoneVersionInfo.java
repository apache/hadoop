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
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneVersionInfo.class);

  private Properties info;

  protected OzoneVersionInfo(String component) {
    info = new Properties();
    String versionInfoFile = component + "-version-info.properties";
    InputStream is = null;
    try {
      is = ThreadUtil
          .getResourceAsStream(OzoneVersionInfo.class.getClassLoader(),
              versionInfoFile);
      info.load(is);
    } catch (IOException ex) {
      LoggerFactory.getLogger(getClass()).warn("Could not read '" +
          versionInfoFile + "', " + ex.toString(), ex);
    } finally {
      IOUtils.closeStream(is);
    }
  }

  protected String getVersion() {
    return info.getProperty("version", "Unknown");
  }

  protected String getRelease() {
    return info.getProperty("release", "Unknown");
  }

  protected String getRevision() {
    return info.getProperty("revision", "Unknown");
  }

  protected String getBranch() {
    return info.getProperty("branch", "Unknown");
  }

  protected String getDate() {
    return info.getProperty("date", "Unknown");
  }

  protected String getUser() {
    return info.getProperty("user", "Unknown");
  }

  protected String getUrl() {
    return info.getProperty("url", "Unknown");
  }

  protected String getSrcChecksum() {
    return info.getProperty("srcChecksum", "Unknown");
  }

  protected String getBuildVersion() {
    return getVersion() +
        " from " + getRevision() +
        " by " + getUser() +
        " source checksum " + getSrcChecksum();
  }

  protected String getProtocVersion() {
    return info.getProperty("protocVersion", "Unknown");
  }

  private static final OzoneVersionInfo OZONE_VERSION_INFO =
      new OzoneVersionInfo("ozone");

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
            "              /    " + OZONE_VERSION_INFO.getVersion() + "("
            + OZONE_VERSION_INFO.getRelease() + ")\n");
    System.out.println(
        "Source code repository " + OZONE_VERSION_INFO.getUrl() + " -r " +
            OZONE_VERSION_INFO.getRevision());
    System.out.println("Compiled by " + OZONE_VERSION_INFO.getUser() + " on "
        + OZONE_VERSION_INFO.getDate());
    System.out.println(
        "Compiled with protoc " + OZONE_VERSION_INFO.getProtocVersion());
    System.out.println(
        "From source with checksum " + OZONE_VERSION_INFO.getSrcChecksum()
            + "\n");
    LOG.debug("This command was run using " +
        ClassUtil.findContainingJar(OzoneVersionInfo.class));
    HddsVersionInfo.main(args);
  }
}
