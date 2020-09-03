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

import java.util.NoSuchElementException;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test the low-level object listing operations.
 */
public class ITestObjectListing extends AbstractS3ATestBase {

  @Test
  public void testAsyncListEmptyPath() throws Throwable {
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    String pathKey = fs.pathToKey(path);
    S3ListRequest r = fs.createListObjectsRequest(pathKey, "/");
    RemoteIterator<S3ListResult> it = fs.getListing()
        .createObjectListingIterator(path, r);

    // for any path on S3, this will return an empty value
    Assertions.assertThat(it.hasNext())
        .describedAs("hasNext() on empty path for iterator: %s", it)
        .isTrue();
    Assertions.assertThat(it.hasNext())
        .describedAs("hasNext() on second call %s", it)
        .isTrue();

    // invoke the result
    S3ListResult result = it.next();
    Assertions.assertThat(result.getObjectSummaries())
        .describedAs("object summaries")
        .isEmpty();
    Assertions.assertThat(result.getCommonPrefixes())
        .describedAs("common prefixes")
        .isEmpty();
    Assertions.assertThat(result.representsEmptyDirectory(null, pathKey, null))
        .describedAs("does this represent an empty directory")
        .isFalse();

    Assertions.assertThat(it.hasNext())
        .describedAs("hasNext() after next() %s", it)
        .isFalse();

    // there are none left.
    intercept(NoSuchElementException.class, () -> it.next());
  }

  @Test
  public void testAsyncListEmptyPath2() throws Throwable {
    describe("Get the list result without calling hasNext first");
    Path path = methodPath();
    S3AFileSystem fs = getFileSystem();
    String pathKey = fs.pathToKey(path);
    S3ListRequest r = fs.createListObjectsRequest(pathKey, "/");
    RemoteIterator<S3ListResult> it = fs.getListing()
        .createObjectListingIterator(path, r);
    // invoke the result
    S3ListResult result = it.next();
    // there are none left.
    intercept(NoSuchElementException.class, () -> it.next());
  }

}
