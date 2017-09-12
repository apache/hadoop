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

package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Tests of the S3A FileSystem which don't have a specific home and can share
 * a filesystem instance with others..
 */
public class ITestS3AMiscOperations extends AbstractS3ATestBase {

  @Test
  public void testCreateNonRecursiveSuccess() throws IOException {
    Path shouldWork = path("nonrecursivenode");
    try(FSDataOutputStream out = createNonRecursive(shouldWork)) {
      out.write(0);
      out.close();
    }
    assertIsFile(shouldWork);
  }

  @Test(expected = FileNotFoundException.class)
  public void testCreateNonRecursiveNoParent() throws IOException {
    createNonRecursive(path("/recursive/node"));
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testCreateNonRecursiveParentIsFile() throws IOException {
    Path parent = path("/file.txt");
    ContractTestUtils.touch(getFileSystem(), parent);
    createNonRecursive(new Path(parent, "fail"));
  }

  @Test
  public void testPutObjectDirect() throws Throwable {
    final S3AFileSystem fs = getFileSystem();
    ObjectMetadata metadata = fs.newObjectMetadata(-1);
    metadata.setContentLength(-1);
    Path path = path("putDirect");
    final PutObjectRequest put = new PutObjectRequest(fs.getBucket(),
        path.toUri().getPath(),
        new ByteArrayInputStream("PUT".getBytes()),
        metadata);
    LambdaTestUtils.intercept(IllegalStateException.class,
        new Callable<PutObjectResult>() {
          @Override
          public PutObjectResult call() throws Exception {
            return fs.putObjectDirect(put);
          }
        });
    assertPathDoesNotExist("put object was created", path);
  }

  private FSDataOutputStream createNonRecursive(Path path) throws IOException {
    return getFileSystem().createNonRecursive(path, false, 4096,
        (short) 3, (short) 4096,
        null);
  }
}
