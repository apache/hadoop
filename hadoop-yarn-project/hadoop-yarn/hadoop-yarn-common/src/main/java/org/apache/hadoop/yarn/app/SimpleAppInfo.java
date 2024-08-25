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

package org.apache.hadoop.yarn.app;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@JsonRootName(value = "SimpleAppInfo")
@XmlRootElement(name = "SimpleAppInfo")
@XmlAccessorType(XmlAccessType.FIELD)
public class SimpleAppInfo {

  private int id;

  private AppState state;

  private String trackingUrl;

  public SimpleAppInfo() {
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public AppState getState() {
    return state;
  }

  public void setState(AppState state) {
    this.state = state;
  }

  public String getTrackingUrl() {
    return trackingUrl;
  }

  public void setTrackingUrl(String trackingUrl) {
    this.trackingUrl = trackingUrl;
  }

  @Override
  public String toString() {
    return "SimpleAppInfo{" +
        "id=" + id +
        ", state=" + state +
        ", trackingUrl='" + trackingUrl + '\'' +
        '}';
  }
}