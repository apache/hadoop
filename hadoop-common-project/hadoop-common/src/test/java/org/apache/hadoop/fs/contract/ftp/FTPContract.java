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

package org.apache.hadoop.fs.contract.ftp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;
import org.junit.Assert;

import java.net.URI;

import static org.junit.Assert.assertNotNull;

/**
 * The contract of FTP; requires the option "test.testdir" to be set
 */
public class FTPContract extends AbstractBondedFSContract {

  public static final String CONTRACT_XML = "contract/ftp.xml";
  /**
   *
   */
  public static final String TEST_FS_TESTDIR = "test.ftp.testdir";
  private String fsName;
  private URI fsURI;
  private FileSystem fs;

  public FTPContract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_XML);
  }

  @Override
  public String getScheme() {
    return "ftp";
  }

  @Override
  public Path getTestPath() {
    String pathString = getOption(TEST_FS_TESTDIR, null);
    assertNotNull("Undefined test option " + TEST_FS_TESTDIR, pathString);
    Path path = new Path(pathString);
    return path;
  }
}
