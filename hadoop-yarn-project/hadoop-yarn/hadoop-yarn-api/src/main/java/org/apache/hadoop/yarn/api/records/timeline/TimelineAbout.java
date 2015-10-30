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

package org.apache.hadoop.yarn.api.records.timeline;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "about")
@XmlAccessorType(XmlAccessType.NONE)
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TimelineAbout {

  private String about;
  private String timelineServiceVersion;
  private String timelineServiceBuildVersion;
  private String timelineServiceVersionBuiltOn;
  private String hadoopVersion;
  private String hadoopBuildVersion;
  private String hadoopVersionBuiltOn;

  public TimelineAbout() {
  }

  public TimelineAbout(String about) {
    this.about = about;
  }

  @XmlElement(name = "About")
  public String getAbout() {
    return about;
  }

  public void setAbout(String about) {
    this.about = about;
  }

  @XmlElement(name = "timeline-service-version")
  public String getTimelineServiceVersion() {
    return timelineServiceVersion;
  }

  public void setTimelineServiceVersion(String timelineServiceVersion) {
    this.timelineServiceVersion = timelineServiceVersion;
  }

  @XmlElement(name = "timeline-service-build-version")
  public String getTimelineServiceBuildVersion() {
    return timelineServiceBuildVersion;
  }

  public void setTimelineServiceBuildVersion(
      String timelineServiceBuildVersion) {
    this.timelineServiceBuildVersion = timelineServiceBuildVersion;
  }

  @XmlElement(name = "timeline-service-version-built-on")
  public String getTimelineServiceVersionBuiltOn() {
    return timelineServiceVersionBuiltOn;
  }

  public void setTimelineServiceVersionBuiltOn(
      String timelineServiceVersionBuiltOn) {
    this.timelineServiceVersionBuiltOn = timelineServiceVersionBuiltOn;
  }

  @XmlElement(name = "hadoop-version")
  public String getHadoopVersion() {
    return hadoopVersion;
  }

  public void setHadoopVersion(String hadoopVersion) {
    this.hadoopVersion = hadoopVersion;
  }

  @XmlElement(name = "hadoop-build-version")
  public String getHadoopBuildVersion() {
    return hadoopBuildVersion;
  }

  public void setHadoopBuildVersion(String hadoopBuildVersion) {
    this.hadoopBuildVersion = hadoopBuildVersion;
  }

  @XmlElement(name = "hadoop-version-built-on")
  public String getHadoopVersionBuiltOn() {
    return hadoopVersionBuiltOn;
  }

  public void setHadoopVersionBuiltOn(String hadoopVersionBuiltOn) {
    this.hadoopVersionBuiltOn = hadoopVersionBuiltOn;
  }
}

