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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

@explain(displayName="Show Tables")
public class showTablesDesc extends ddlDesc implements Serializable 
{
  private static final long serialVersionUID = 1L;
  String     pattern;
  File       resFile;
  
  /**
   * @param resFile
   */
  public showTablesDesc(File resFile) {
    this.resFile = resFile;
    pattern = null;
  }

  /**
   * @param pattern names of tables to show
   */
  public showTablesDesc(File resFile, String pattern) {
    this.resFile = resFile;
    this.pattern = pattern;
  }

  /**
   * @return the pattern
   */
  @explain(displayName="pattern")
  public String getPattern() {
    return pattern;
  }

  /**
   * @param pattern the pattern to set
   */
  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  /**
   * @return the resFile
   */
  public File getResFile() {
    return resFile;
  }

  @explain(displayName="result file", normalExplain=false)
  public String getResFileString() {
    try {
      return getResFile().getCanonicalPath();
    }
    catch (IOException ioe) {
      return "error";
    }
  }
  /**
   * @param resFile the resFile to set
   */
  public void setResFile(File resFile) {
    this.resFile = resFile;
  }
}
