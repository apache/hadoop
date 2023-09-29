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

package org.apache.hadoop.fs.contract.router.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Test dir operations on a Router WebHDFS FS.
 */
public class TestRouterWebHDFSContractRootDirectory extends
    AbstractContractRootDirectoryTest {

  @BeforeClass
  public static void createCluster() throws IOException {
    RouterWebHDFSContract.createCluster();
  }

  @AfterClass
  public static void teardownCluster() throws IOException {
    RouterWebHDFSContract.destroyCluster();
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new RouterWebHDFSContract(conf);
  }

  @Override
  public void testListEmptyRootDirectory() throws IOException {
    // It doesn't apply because we still have the mount points here
  }

  @Override
  public void testRmEmptyRootDirNonRecursive() throws IOException {
    // It doesn't apply because we still have the mount points here
  }

  @Override
  public void testRecursiveRootListing() throws IOException {
    // It doesn't apply because we still have the mount points here
  }

  @Override
  public void testRmRootRecursive() {
    // It doesn't apply because we still have the mount points here
  }

  @Override
  public void testRmEmptyRootDirRecursive() {
    // It doesn't apply because we still have the mount points here
  }

  @Override
  public void testSimpleRootListing() {
    // It doesn't apply because DFSRouter dosn't support LISTSTATUS_BATCH.
  }
}
