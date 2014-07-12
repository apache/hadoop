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

package org.apache.hadoop.fs.contract.rawlocal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.contract.localfs.LocalFSContract;

import java.io.File;
import java.io.IOException;

/**
 * Raw local filesystem. This is the inner OS-layer FS
 * before checksumming is added around it.
 */
public class RawlocalFSContract extends LocalFSContract {
  public RawlocalFSContract(Configuration conf) {
    super(conf);
  }

  public static final String RAW_CONTRACT_XML = "contract/localfs.xml";

  @Override
  protected String getContractXml() {
    return RAW_CONTRACT_XML;
  }

  @Override
  protected FileSystem getLocalFS() throws IOException {
    return FileSystem.getLocal(getConf()).getRawFileSystem();
  }

  public File getTestDirectory() {
    return new File(getTestDataDir());
  }
}
