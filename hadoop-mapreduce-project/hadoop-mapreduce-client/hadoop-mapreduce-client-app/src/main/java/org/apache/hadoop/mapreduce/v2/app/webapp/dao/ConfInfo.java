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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.util.MRJobConfUtil;
import org.apache.hadoop.mapreduce.v2.app.job.Job;

@XmlRootElement(name = "conf")
@XmlAccessorType(XmlAccessType.FIELD)
public class ConfInfo {

  protected String path;
  protected ArrayList<ConfEntryInfo> property;

  public ConfInfo() {
  }

  public ConfInfo(Job job) throws IOException {

    this.property = new ArrayList<ConfEntryInfo>();
    Configuration jobConf = job.loadConfFile();
    this.path = job.getConfFile().toString();
    MRJobConfUtil.redact(jobConf);
    for (Map.Entry<String, String> entry : jobConf) {
      this.property.add(new ConfEntryInfo(entry.getKey(), entry.getValue(), 
          jobConf.getPropertySources(entry.getKey())));
    }

  }

  public ArrayList<ConfEntryInfo> getProperties() {
    return this.property;
  }

  public String getPath() {
    return this.path;
  }

}
