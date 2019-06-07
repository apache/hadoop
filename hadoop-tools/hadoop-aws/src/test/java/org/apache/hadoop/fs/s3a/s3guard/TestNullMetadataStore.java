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

package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * Run MetadataStore unit tests on the NullMetadataStore implementation.
 */
public class TestNullMetadataStore extends MetadataStoreTestBase {
  private static class NullMSContract extends AbstractMSContract {
    @Override
    public FileSystem getFileSystem() throws IOException {
      Configuration config = new Configuration();
      return FileSystem.getLocal(config);
    }

    @Override
    public MetadataStore getMetadataStore() throws IOException {
      return new NullMetadataStore();
    }
  }

  /** This MetadataStore always says "I don't know, ask the backing store". */
  @Override
  public boolean allowMissing() {
    return true;
  }

  @Override protected String getPathStringForPrune(String path)
      throws Exception {
    return path;
  }

  @Override
  public AbstractMSContract createContract() {
    return new NullMSContract();
  }

  @Override
  public AbstractMSContract createContract(Configuration conf) {
    return createContract();
  }
}
