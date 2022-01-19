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

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Place for the S3A listing classes; keeps all the small classes under control.
 */
public class TestListing extends AbstractS3AMockTest {

  @Test
  public void testProvidedFileStatusIteratorEnd() throws Exception {
    S3AFileStatus s3aStatus = new S3AFileStatus(
        100, 0, new Path("s3a://blah/blah"),
        8192, null, null, null);

    S3AFileStatus[] statuses = {
        s3aStatus
    };
    RemoteIterator<S3AFileStatus> it = Listing.toProvidedFileStatusIterator(
        statuses);

    Assert.assertTrue("hasNext() should return true first time", it.hasNext());
    Assert.assertEquals("first element from iterator",
        s3aStatus, it.next());
    Assert.assertFalse("hasNext() should now be false", it.hasNext());
    intercept(NoSuchElementException.class, it::next);
  }
}
