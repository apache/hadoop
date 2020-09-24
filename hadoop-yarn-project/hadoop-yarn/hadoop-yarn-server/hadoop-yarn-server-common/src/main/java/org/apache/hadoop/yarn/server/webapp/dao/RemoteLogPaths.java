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
package org.apache.hadoop.yarn.server.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Container of a list of {@link RemoteLogPathEntry}.
 */
@XmlRootElement(name = "remoteLogDirPathResult")
@XmlAccessorType(XmlAccessType.FIELD)
public class RemoteLogPaths {

  @XmlElement(name = "paths")
  private List<RemoteLogPathEntry> paths;

  //JAXB needs this
  public RemoteLogPaths() {}

  public RemoteLogPaths(List<RemoteLogPathEntry> paths) {
    this.paths = paths;
  }

  public List<RemoteLogPathEntry> getPaths() {
    return paths;
  }

  public void setPaths(List<RemoteLogPathEntry> paths) {
    this.paths = paths;
  }
}