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

import java.io.Serializable;
import org.apache.hadoop.fs.Path;

@explain(displayName="Show Partitions")
public class showPartitionsDesc extends ddlDesc implements Serializable 
{
  private static final long serialVersionUID = 1L;
  String     tabName;
  Path       resFile;
  
  /**
   * @param tabName Name of the table whose partitions need to be listed
   * @param resFile File to store the results in
   */
  public showPartitionsDesc(String tabName, Path resFile) {
    this.tabName = tabName;
    this.resFile = resFile;
  }

  /**
   * @return the name of the table
   */
  @explain(displayName="table")
  public String getTabName() {
    return tabName;
  }

  /**
   * @param tabName the table whose partitions have to be listed
   */
  public void setTabName(String tabName) {
    this.tabName = tabName;
  }

  /**
   * @return the results file
   */
  public Path getResFile() {
    return resFile;
  }

  @explain(displayName="result file", normalExplain=false)
  public String getResFileString() {
    return getResFile().getName();
  }
  /**
   * @param resFile the results file to be used to return the results
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
}
