/*
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

package org.apache.hadoop.fs.bos.contract;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * Contract for Baidu BOS FileSystem.
 * This defines the filesystem capabilities and contract for BOS.
 */
public class BosContract extends AbstractFSContract {

  public static final String CONTRACT_XML = "contract/bos.xml";

  /**
   * Create a new BOS contract.
   * @param conf configuration to use
   */
  protected BosContract(Configuration conf) {
    super(conf);
    // Load the BOS contract configuration
    addConfResource(CONTRACT_XML);
  }

  /**
   * Get the filesystem scheme: "bos".
   * @return "bos"
   */
  @Override
  public String getScheme() {
    return "bos";
  }

  /**
   * Get the path for testing.
   * @return test path
   */
  @Override
  public Path getTestPath() {
    // Get the test filesystem URI from configuration
    String testFsUri = getConf().getTrimmed("fs.contract.test.fs.bos", "");
    if (testFsUri.isEmpty()) {
      throw new IllegalStateException(
        "fs.contract.test.fs.bos is not set in configuration. " +
        "Please configure it in contract-test-options.xml"
      );
    }
    return new Path(testFsUri);
  }

  /**
   * Get the test filesystem.
   * @return the test filesystem
   */
  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return FileSystem.get(getTestPath().toUri(), getConf());
  }
}
