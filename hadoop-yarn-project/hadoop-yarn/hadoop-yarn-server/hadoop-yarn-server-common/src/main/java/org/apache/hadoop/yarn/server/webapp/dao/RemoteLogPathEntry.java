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

/**
 * A remote log path for a log aggregation file controller.
 * <pre>
 *   {@code <ROOT_PATH>/%USER/<SUFFIX>}
 * </pre>
 */
public class RemoteLogPathEntry {
  private String fileController;
  private String path;

  //JAXB needs this
  public RemoteLogPathEntry() {}

  public RemoteLogPathEntry(String fileController, String path) {
    this.fileController = fileController;
    this.path = path;
  }

  public String getFileController() {
    return fileController;
  }

  public void setFileController(String fileController) {
    this.fileController = fileController;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }
}
