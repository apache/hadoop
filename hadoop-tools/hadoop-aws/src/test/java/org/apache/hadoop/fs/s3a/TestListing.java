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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.hadoop.fs.s3a.S3AUtils.ACCEPT_ALL;
import static org.apache.hadoop.fs.s3a.Listing.ProvidedFileStatusIterator;

/**
 * Place for the S3A listing classes; keeps all the small classes under control.
 */
public class TestListing extends AbstractS3AMockTest {

  private static class MockRemoteIterator<S3AFileStatus> implements
      RemoteIterator<S3AFileStatus> {
    private Iterator<S3AFileStatus> iterator;

    MockRemoteIterator(Collection<S3AFileStatus> source) {
      iterator = source.iterator();
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public S3AFileStatus next() {
      return iterator.next();
    }
  }

  private S3AFileStatus blankFileStatus(Path path) {
    return new S3AFileStatus(Tristate.UNKNOWN, path, null);
  }

  @Test
  public void testTombstoneReconcilingIterator() throws Exception {
    Path parent = new Path("/parent");
    Path liveChild = new Path(parent, "/liveChild");
    Path deletedChild = new Path(parent, "/deletedChild");
    Path[] allFiles = {parent, liveChild, deletedChild};
    Path[] liveFiles = {parent, liveChild};

    Listing listing = new Listing(fs);
    Collection<FileStatus> statuses = new ArrayList<>();
    statuses.add(blankFileStatus(parent));
    statuses.add(blankFileStatus(liveChild));
    statuses.add(blankFileStatus(deletedChild));

    Set<Path> tombstones = new HashSet<>();
    tombstones.add(deletedChild);

    RemoteIterator<S3AFileStatus> sourceIterator = new MockRemoteIterator(
        statuses);
    RemoteIterator<S3ALocatedFileStatus> locatedIterator =
        listing.createLocatedFileStatusIterator(sourceIterator);
    RemoteIterator<S3ALocatedFileStatus> reconcilingIterator =
        listing.createTombstoneReconcilingIterator(locatedIterator, tombstones);

    Set<Path> expectedPaths = new HashSet<>();
    expectedPaths.add(parent);
    expectedPaths.add(liveChild);

    Set<Path> actualPaths = new HashSet<>();
    while (reconcilingIterator.hasNext()) {
      actualPaths.add(reconcilingIterator.next().getPath());
    }
    Assert.assertTrue(actualPaths.equals(expectedPaths));
  }

  @Test
  public void testProvidedFileStatusIteratorEnd() throws Exception {
    S3AFileStatus s3aStatus = new S3AFileStatus(
        100, 0, new Path("s3a://blah/blah"),
        8192, null, null, null);

    S3AFileStatus[] statuses = {
        s3aStatus
    };
    ProvidedFileStatusIterator it = new ProvidedFileStatusIterator(statuses,
        ACCEPT_ALL, new Listing.AcceptAllButS3nDirs());

    Assert.assertTrue("hasNext() should return true first time", it.hasNext());
    Assert.assertNotNull("first element should not be null", it.next());
    Assert.assertFalse("hasNext() should now be false", it.hasNext());
    try {
      it.next();
      Assert.fail("next() should have thrown exception");
    } catch (NoSuchElementException e) {
      // Correct behavior.  Any other exceptions are propagated as failure.
      return;
    }
  }
}
