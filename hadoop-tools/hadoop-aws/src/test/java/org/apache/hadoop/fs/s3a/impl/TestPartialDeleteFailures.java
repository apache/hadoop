/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a.impl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ATestUtils;

import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.ACCESS_DENIED;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.removeUndeletedPaths;
import static org.junit.Assert.assertEquals;

/**
 * Unit test suite covering translation of AWS SDK exceptions to S3A exceptions,
 * and retry/recovery policies.
 */
public class TestPartialDeleteFailures {

  private static final ContextAccessors CONTEXT_ACCESSORS
      = new MinimalContextAccessor();

  private StoreContext context;

  private static Path qualifyKey(String k) {
    return new Path("s3a://bucket/" + k);
  }

  @Before
  public void setUp() throws Exception {
    context = S3ATestUtils.createMockStoreContext(true,
        new S3ATestUtils.OperationTrackingStore(), CONTEXT_ACCESSORS);
  }

  @Test
  public void testDeleteExtraction() {
    List<Path> src = pathList("a", "a/b", "a/c");
    List<Path> rejected = pathList("a/b");
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        rejected);
    List<Path> undeleted = removeUndeletedPaths(ex, src,
        TestPartialDeleteFailures::qualifyKey);
    assertEquals("mismatch of rejected and undeleted entries",
        rejected, undeleted);
  }

  @Test
  public void testSplitKeysFromResults() throws Throwable {
    List<Path> src = pathList("a", "a/b", "a/c");
    List<Path> rejected = pathList("a/b");
    List<DeleteObjectsRequest.KeyVersion> keys = keysToDelete(src);
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        rejected);
    Pair<List<Path>, List<Path>> pair =
        new MultiObjectDeleteSupport(context, null)
          .splitUndeletedKeys(ex, keys);
    List<Path> undeleted = pair.getLeft();
    List<Path> deleted = pair.getRight();
    assertEquals(rejected, undeleted);
    // now check the deleted list to verify that it is valid
    src.remove(rejected.get(0));
    assertEquals(src, deleted);
  }

  /**
   * Build a list of qualified paths from vararg parameters.
   * @param paths paths to qualify and then convert to a lst.
   * @return same paths as a list.
   */
  private List<Path> pathList(String... paths) {
    return Arrays.stream(paths)
        .map(TestPartialDeleteFailures::qualifyKey)
        .collect(Collectors.toList());
  }

  /**
   * Build a delete exception containing all the rejected paths.
   * The list of successful entries is empty.
   * @param rejected the rejected paths.
   * @return a new exception
   */
  private MultiObjectDeleteException createDeleteException(
      final String code,
      final List<Path> rejected) {
    List<MultiObjectDeleteException.DeleteError> errors = rejected.stream()
        .map((p) -> {
          MultiObjectDeleteException.DeleteError e
              = new MultiObjectDeleteException.DeleteError();
          e.setKey(p.toUri().getPath());
          e.setCode(code);
          e.setMessage("forbidden");
          return e;
        }).collect(Collectors.toList());
    return new MultiObjectDeleteException(errors, Collections.emptyList());
  }

  /**
   * From a list of paths, build up the list of keys for a delete request.
   * @param paths path list
   * @return a key list suitable for a delete request.
   */
  public static List<DeleteObjectsRequest.KeyVersion> keysToDelete(
      List<Path> paths) {
    return paths.stream()
        .map((p) -> p.toUri().getPath())
        .map(DeleteObjectsRequest.KeyVersion::new)
        .collect(Collectors.toList());
  }

  /**
   * Verify that on a partial delete, the S3Guard tables are updated
   * with deleted items. And only them.
   */
  @Test
  public void testProcessDeleteFailure() throws Throwable {
    Path pathA = qualifyKey("/a");
    Path pathAB = qualifyKey("/a/b");
    Path pathAC = qualifyKey("/a/c");
    List<Path> src = Lists.newArrayList(pathA, pathAB, pathAC);
    List<DeleteObjectsRequest.KeyVersion> keyList = keysToDelete(src);
    List<Path> deleteForbidden = Lists.newArrayList(pathAB);
    final List<Path> deleteAllowed = Lists.newArrayList(pathA, pathAC);
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        deleteForbidden);
    S3ATestUtils.OperationTrackingStore store
        = new S3ATestUtils.OperationTrackingStore();
    StoreContext storeContext = S3ATestUtils
            .createMockStoreContext(true, store, CONTEXT_ACCESSORS);
    MultiObjectDeleteSupport deleteSupport
        = new MultiObjectDeleteSupport(storeContext, null);
    Triple<List<Path>, List<Path>, List<Pair<Path, IOException>>>
        triple = deleteSupport.processDeleteFailure(ex, keyList);
    Assertions.assertThat(triple.getRight())
        .as("failure list")
        .isEmpty();
    List<Path> undeleted = triple.getLeft();
    List<Path> deleted = triple.getMiddle();
    Assertions.assertThat(deleted).
        as("deleted files")
        .containsAll(deleteAllowed)
        .doesNotContainAnyElementsOf(deleteForbidden);
    Assertions.assertThat(undeleted).
        as("undeleted store entries")
        .containsAll(deleteForbidden)
        .doesNotContainAnyElementsOf(deleteAllowed);
  }


  private static class MinimalContextAccessor implements ContextAccessors {

    @Override
    public Path keyToPath(final String key) {
      return qualifyKey(key);
    }

    @Override
    public String pathToKey(final Path path) {
      return null;
    }

    @Override
    public File createTempFile(final String prefix, final long size)
        throws IOException {
      throw new UnsupportedOperationException("unsppported");
    }

    @Override
    public String getBucketLocation() throws IOException {
      return null;
    }

    @Override
    public Path makeQualified(final Path path) {
      return path;
    }
  }

}
