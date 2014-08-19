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

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.net.URI;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

@XmlRootElement(name = "localresources")
@XmlAccessorType(XmlAccessType.FIELD)
public class LocalResourceInfo {

  @XmlElement(name = "resource")
  URI url;
  LocalResourceType type;
  LocalResourceVisibility visibility;
  long size;
  long timestamp;
  String pattern;

  public URI getUrl() {
    return url;
  }

  public LocalResourceType getType() {
    return type;
  }

  public LocalResourceVisibility getVisibility() {
    return visibility;
  }

  public long getSize() {
    return size;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getPattern() {
    return pattern;
  }

  public void setUrl(URI url) {
    this.url = url;
  }

  public void setType(LocalResourceType type) {
    this.type = type;
  }

  public void setVisibility(LocalResourceVisibility visibility) {
    this.visibility = visibility;
  }

  public void setSize(long size) {
    if (size <= 0) {
      throw new IllegalArgumentException("size must be greater than 0");
    }
    this.size = size;
  }

  public void setTimestamp(long timestamp) {
    if (timestamp <= 0) {
      throw new IllegalArgumentException("timestamp must be greater than 0");
    }
    this.timestamp = timestamp;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }
}
