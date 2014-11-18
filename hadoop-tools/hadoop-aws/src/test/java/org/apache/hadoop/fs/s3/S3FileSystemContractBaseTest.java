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

package org.apache.hadoop.fs.s3;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.junit.internal.AssumptionViolatedException;

public abstract class S3FileSystemContractBaseTest
  extends FileSystemContractBaseTest {

  public static final String KEY_TEST_FS = "test.fs.s3.name";
  private FileSystemStore store;
  
  abstract FileSystemStore getFileSystemStore() throws IOException;
  
  @Override
  protected void setUp() throws Exception {
    Configuration conf = new Configuration();
    store = getFileSystemStore();
    fs = new S3FileSystem(store);
    String fsname = conf.get(KEY_TEST_FS);
    if (StringUtils.isEmpty(fsname)) {
      throw new AssumptionViolatedException(
          "No test FS defined in :" + KEY_TEST_FS);
    }
    fs.initialize(URI.create(fsname), conf);
  }
  
  @Override
  protected void tearDown() throws Exception {
    store.purge();
    super.tearDown();
  }
  
  public void testCanonicalName() throws Exception {
    assertNull("s3 doesn't support security token and shouldn't have canonical name",
               fs.getCanonicalServiceName());
  }

}
