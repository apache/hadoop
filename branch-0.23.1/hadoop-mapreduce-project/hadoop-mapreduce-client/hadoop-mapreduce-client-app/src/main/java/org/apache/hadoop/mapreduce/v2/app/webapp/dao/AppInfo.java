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
package org.apache.hadoop.mapreduce.v2.app.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.yarn.util.Times;

@XmlRootElement(name = "info")
@XmlAccessorType(XmlAccessType.FIELD)
public class AppInfo {

  protected String appId;
  protected String name;
  protected String user;
  protected long startedOn;
  protected long elapsedTime;

  public AppInfo() {
  }

  public AppInfo(App app, AppContext context) {
    this.appId = context.getApplicationID().toString();
    this.name = context.getApplicationName().toString();
    this.user = context.getUser().toString();
    this.startedOn = context.getStartTime();
    this.elapsedTime = Times.elapsed(this.startedOn, 0);
  }

  public String getId() {
    return this.appId;
  }

  public String getName() {
    return this.name;
  }

  public String getUser() {
    return this.user;
  }

  public long getStartTime() {
    return this.startedOn;
  }

  public long getElapsedTime() {
    return this.elapsedTime;
  }

}
