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

package org.apache.hadoop.mapreduce.v2.hs.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer;
import org.apache.hadoop.util.VersionInfo;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class HistoryInfo {

  protected long startedOn;
  protected String hadoopVersion;
  protected String hadoopBuildVersion;
  protected String hadoopVersionBuiltOn;

  public HistoryInfo() {
    this.startedOn = JobHistoryServer.historyServerTimeStamp;
    this.hadoopVersion = VersionInfo.getVersion();
    this.hadoopBuildVersion = VersionInfo.getBuildVersion();
    this.hadoopVersionBuiltOn = VersionInfo.getDate();
  }

  public String getHadoopVersion() {
    return this.hadoopVersion;
  }

  public String getHadoopBuildVersion() {
    return this.hadoopBuildVersion;
  }

  public String getHadoopVersionBuiltOn() {
    return this.hadoopVersionBuiltOn;
  }

  public long getStartedOn() {
    return this.startedOn;
  }

}
