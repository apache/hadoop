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

package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGContext;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.util.YarnVersionInfo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class GpgInfo {
  private String gpgVersion;
  private String gpgBuildVersion;
  private String gpgVersionBuiltOn;
  private String hadoopVersion;
  private String hadoopBuildVersion;
  private String hadoopVersionBuiltOn;
  private long gpgStartupTime;

  public GpgInfo() {
  } // JAXB needs this

  public GpgInfo(final GPGContext context) {
    this.gpgVersion = YarnVersionInfo.getVersion();
    this.gpgBuildVersion = YarnVersionInfo.getBuildVersion();
    this.gpgVersionBuiltOn = YarnVersionInfo.getDate();
    this.hadoopVersion = VersionInfo.getVersion();
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
    this.gpgStartupTime = GlobalPolicyGenerator.getGPGStartupTime();
  }

  public String getGpgVersion() {
    return gpgVersion;
  }

  public String getGpgBuildVersion() {
    return gpgBuildVersion;
  }

  public String getGpgVersionBuiltOn() {
    return gpgVersionBuiltOn;
  }

  public String getHadoopVersion() {
    return hadoopVersion;
  }

  public String getHadoopBuildVersion() {
    return hadoopBuildVersion;
  }

  public String getHadoopVersionBuiltOn() {
    return hadoopVersionBuiltOn;
  }

  public long getGpgStartupTime() {
    return gpgStartupTime;
  }
}
