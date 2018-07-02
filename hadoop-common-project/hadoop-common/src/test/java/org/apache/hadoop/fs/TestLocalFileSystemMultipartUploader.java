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
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.test.GenericTestUtils.getRandomizedTestDir;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

/**
 * Test the FileSystemMultipartUploader on local file system.
 */
public class TestLocalFileSystemMultipartUploader
    extends AbstractSystemMultipartUploaderTest {

  private static FileSystem fs;
  private File tmp;

  @BeforeClass
  public static void init() throws IOException {
    fs = LocalFileSystem.getLocal(new Configuration());
  }

  @Before
  public void setup() throws IOException {
    tmp = getRandomizedTestDir();
    tmp.mkdirs();
  }

  @After
  public void tearDown() throws IOException {
    tmp.delete();
  }

  @Override
  public FileSystem getFS() {
    return fs;
  }

  @Override
  public Path getBaseTestPath() {
    return new Path(tmp.getAbsolutePath());
  }

}