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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ITestS3AUrlScheme extends AbstractS3ATestBase{
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    return conf;
  }

  @Test
  public void testFSScheme() throws IOException, URISyntaxException {
    FileSystem fs = FileSystem.get(new URI("s3://mybucket/path"),
        getConfiguration());
    try {
      assertEquals("s3", fs.getScheme());
      Path path = fs.makeQualified(new Path("tmp/path"));
      assertEquals("s3", path.toUri().getScheme());
    } finally {
      fs.close();
    }
  }
}
