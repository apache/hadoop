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

package org.apache.hadoop.fs.azurebfs.extensions;

import org.apache.hadoop.conf.Configuration;

/**
 * A mock SAS token provider to test error conditions.
 */
public class MockInvalidSASTokenProvider implements SASTokenProvider {
  private final String invalidSASToken = "testInvalidSASToken";

  @Override
  public void initialize(Configuration configuration, String accountName) {
    //do nothing
  }

  /**
   * Returns null SAS token query or Empty if returnEmptySASToken is set.
   * @param accountName
   * @param fileSystem the name of the fileSystem.
   * @param path the file or directory path.
   * @param operation the operation to be performed on the path.
   * @return
   */
  @Override
  public String getSASToken(String accountName, String fileSystem, String path,
      String operation) {
    return invalidSASToken;
  }

}
