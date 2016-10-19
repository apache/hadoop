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
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;

/**
 * MetadataStore unit test for {@link LocalMetadataStore}.
 */
public class TestLocalMetadataStore extends MetadataStoreTestBase {

  private static final String MAX_ENTRIES_STR = "16";

  private static class LocalMSContract extends AbstractMSContract {

    private FileSystem fs;

    public LocalMSContract() {
      fs = new RawLocalFileSystem();
      Configuration config = fs.getConf();
      if (config == null) {
        config = new Configuration();
      }
      config.set(LocalMetadataStore.CONF_MAX_RECORDS, MAX_ENTRIES_STR);
      fs.setConf(config);
    }

    @Override
    public FileSystem getFileSystem() {
      return fs;
    }

    @Override
    public MetadataStore getMetadataStore() throws IOException {
      LocalMetadataStore lms = new LocalMetadataStore();
      return lms;
    }
  }

  @Override
  public AbstractMSContract createContract() {
    return new LocalMSContract();
  }

}