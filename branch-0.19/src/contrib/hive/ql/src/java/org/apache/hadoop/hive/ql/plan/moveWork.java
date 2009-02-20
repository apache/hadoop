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

package org.apache.hadoop.hive.ql.plan;

import java.util.*;
import java.io.*;

@explain(displayName="Move Operator")
public class moveWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private List<loadTableDesc> loadTableWork;
  private List<loadFileDesc> loadFileWork;

  public moveWork() { }
  public moveWork(
    final List<loadTableDesc> loadTableWork,
    final List<loadFileDesc> loadFileWork) {
    this.loadTableWork = loadTableWork;
    this.loadFileWork = loadFileWork;
  }
  @explain(displayName="tables")
  public List<loadTableDesc> getLoadTableWork() {
    return this.loadTableWork;
  }
  public void setLoadTableWork(final List<loadTableDesc> loadTableWork) {
    this.loadTableWork = loadTableWork;
  }
  
  @explain(displayName="files")
  public List<loadFileDesc> getLoadFileWork() {
    return this.loadFileWork;
  }
  public void setLoadFileWork(final List<loadFileDesc> loadFileWork) {
    this.loadFileWork=loadFileWork;
  }
}
