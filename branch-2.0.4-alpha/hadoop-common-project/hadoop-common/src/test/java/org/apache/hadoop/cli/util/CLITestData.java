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

package org.apache.hadoop.cli.util;

import java.util.ArrayList;

/**
 *
 * Class to store CLI Test Data
 */
public class CLITestData {
  private String testDesc = null;
  private ArrayList<CLICommand> testCommands = null;
  private ArrayList<CLICommand> cleanupCommands = null;
  private ArrayList<ComparatorData> comparatorData = null;
  private boolean testResult = false;
  
  public CLITestData() {

  }

  /**
   * @return the testDesc
   */
  public String getTestDesc() {
    return testDesc;
  }

  /**
   * @param testDesc the testDesc to set
   */
  public void setTestDesc(String testDesc) {
    this.testDesc = testDesc;
  }

  /**
   * @return the testCommands
   */
  public ArrayList<CLICommand> getTestCommands() {
    return testCommands;
  }

  /**
   * @param testCommands the testCommands to set
   */
  public void setTestCommands(ArrayList<CLICommand> testCommands) {
    this.testCommands = testCommands;
  }

  /**
   * @return the comparatorData
   */
  public ArrayList<ComparatorData> getComparatorData() {
    return comparatorData;
  }

  /**
   * @param comparatorData the comparatorData to set
   */
  public void setComparatorData(ArrayList<ComparatorData> comparatorData) {
    this.comparatorData = comparatorData;
  }

  /**
   * @return the testResult
   */
  public boolean getTestResult() {
    return testResult;
  }

  /**
   * @param testResult the testResult to set
   */
  public void setTestResult(boolean testResult) {
    this.testResult = testResult;
  }

  /**
   * @return the cleanupCommands
   */
  public ArrayList<CLICommand> getCleanupCommands() {
    return cleanupCommands;
  }

  /**
   * @param cleanupCommands the cleanupCommands to set
   */
  public void setCleanupCommands(ArrayList<CLICommand> cleanupCommands) {
    this.cleanupCommands = cleanupCommands;
  }
}
