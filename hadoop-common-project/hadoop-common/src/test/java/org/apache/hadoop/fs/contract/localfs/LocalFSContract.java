/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.localfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractOptions;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.util.Shell;

import java.io.IOException;

/**
 * The contract of the Local filesystem.
 * This changes its feature set from platform for platform -the default
 * set is updated during initialization.
 *
 * This contract contains some override points, to permit
 * the raw local filesystem and other filesystems to subclass it.
 */
public class LocalFSContract extends AbstractFSContract {

  public static final String CONTRACT_XML = "contract/localfs.xml";
  private FileSystem fs;
  private String testDataDir = new FileSystemTestHelper().getTestRootDir();

  public LocalFSContract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(getContractXml());
  }

  /**
   * Return the contract file for this filesystem
   * @return the XML
   */
  protected String getContractXml() {
    return CONTRACT_XML;
  }

  @Override
  public void init() throws IOException {
    super.init();
    fs = getLocalFS();
    adjustContractToLocalEnvironment();
  }

  /**
   *  tweak some of the contract parameters based on the local system
   *  state
   */
  protected void adjustContractToLocalEnvironment() {
    if (Shell.WINDOWS) {
      //NTFS doesn't do case sensitivity, and its permissions are ACL-based
      getConf().setBoolean(getConfKey(ContractOptions.IS_CASE_SENSITIVE), false);
      getConf().setBoolean(getConfKey(ContractOptions.SUPPORTS_UNIX_PERMISSIONS), false);
    } else if (ContractTestUtils.isOSX()) {
      //OSX HFS+ is not case sensitive
      getConf().setBoolean(getConfKey(ContractOptions.IS_CASE_SENSITIVE),
                           false);
    }
  }

  /**
   * Get the local filesystem. This may be overridden
   * @return the filesystem
   * @throws IOException
   */
  protected FileSystem getLocalFS() throws IOException {
    return FileSystem.getLocal(getConf());
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    return fs;
  }

  @Override
  public String getScheme() {
    return "file";
  }

  @Override
  public Path getTestPath() {
    Path path = fs.makeQualified(new Path(
      getTestDataDir()));
    return path;
  }

  /**
   * Get the test data directory
   * @return the directory for test data
   */
  protected String getTestDataDir() {
    return testDataDir;
  }
}
