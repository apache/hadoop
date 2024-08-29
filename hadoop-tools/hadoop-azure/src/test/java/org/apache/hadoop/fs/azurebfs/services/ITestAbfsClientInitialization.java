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

package org.apache.hadoop.fs.azurebfs.services;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAbfsClientInitialization extends AbstractAbfsIntegrationTest {

  public ITestAbfsClientInitialization() throws Exception{

  }

  @Test
  public void testFileSystemInitFailsWithBlobEndpoitUrl() throws Exception {
    Configuration configuration = getRawConfiguration();
    String defaultUri = configuration.get(FS_DEFAULT_NAME_KEY);
    String blobUri = defaultUri.replace(ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME);
    intercept(InvalidConfigurationValueException.class,
        "Blob Endpoint Support not yet available", () ->
            FileSystem.newInstance(new Path(blobUri).toUri(), configuration));
  }
}
