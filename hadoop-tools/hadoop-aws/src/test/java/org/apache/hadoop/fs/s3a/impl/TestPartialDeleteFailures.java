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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.test.OperationTrackingStore;

import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.ACCESS_DENIED;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.removeUndeletedPaths;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.toPathList;
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

  private static String toKey(Path path) {
    return path.toUri().getPath();
  }

  @Before
  public void setUp() throws Exception {
    context = S3ATestUtils.createMockStoreContext(true,
        new OperationTrackingStore(), CONTEXT_ACCESSORS);
  }

  @Test
  public void testDeleteExtraction() {
    List<MultiObjectDeleteSupport.KeyPath> src = pathList("a", "a/b", "a/c");
    List<MultiObjectDeleteSupport.KeyPath> rejected = pathList("a/b");
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        rejected);
    List<MultiObjectDeleteSupport.KeyPath> undeleted =
        removeUndeletedPaths(ex, src,
            TestPartialDeleteFailures::qualifyKey);
    assertEquals("mismatch of rejected and undeleted entries",
        rejected, undeleted);
  }

  @Test
  public void testSplitKeysFromResults() throws Throwable {
    List<MultiObjectDeleteSupport.KeyPath> src = pathList("a", "a/b", "a/c");
    List<MultiObjectDeleteSupport.KeyPath> rejected = pathList("a/b");
    List<DeleteObjectsRequest.KeyVersion> keys = keysToDelete(toPathList(src));
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        rejected);
    Pair<List<MultiObjectDeleteSupport.KeyPath>,
        List<MultiObjectDeleteSupport.KeyPath>> pair =
        new MultiObjectDeleteSupport(context, null)
          .splitUndeletedKeys(ex, keys);
    List<MultiObjectDeleteSupport.KeyPath> undeleted = pair.getLeft();
    List<MultiObjectDeleteSupport.KeyPath> deleted = pair.getRight();
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
  private List<MultiObjectDeleteSupport.KeyPath> pathList(String... paths) {
    return Arrays.stream(paths)
        .map(k->
            new MultiObjectDeleteSupport.KeyPath(k,
                qualifyKey(k),
                k.endsWith("/")))
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
      final List<MultiObjectDeleteSupport.KeyPath> rejected) {
    List<MultiObjectDeleteException.DeleteError> errors = rejected.stream()
        .map((kp) -> {
          Path p = kp.getPath();
          MultiObjectDeleteException.DeleteError e
              = new MultiObjectDeleteException.DeleteError();
          e.setKey(kp.getKey());
          e.setCode(code);
          e.setMessage("forbidden");
          return e;
        }).collect(Collectors.toList());
    return new MultiObjectDeleteException(errors, Collections.emptyList());
  }

  /**
   * From a list of paths, build up the list of KeyVersion records
   * for a delete request.
   * All the entries will be files (i.e. no trailing /)
   * @param paths path list
   * @return a key list suitable for a delete request.
   */
  public static List<DeleteObjectsRequest.KeyVersion> keysToDelete(
      List<Path> paths) {
    return paths.stream()
        .map(p -> {
          String uripath = p.toUri().getPath();
          return uripath.substring(1);
        })
        .map(DeleteObjectsRequest.KeyVersion::new)
        .collect(Collectors.toList());
  }

  /**
   * From a list of keys, build up the list of keys for a delete request.
   * If a key has a trailing /, that will be retained, so it will be
   * considered a directory during multi-object delete failure handling
   * @param keys key list
   * @return a key list suitable for a delete request.
   */
  public static List<DeleteObjectsRequest.KeyVersion> toDeleteRequests(
      List<String> keys) {
    return keys.stream()
        .map(DeleteObjectsRequest.KeyVersion::new)
        .collect(Collectors.toList());
  }

  /**
   * Verify that on a partial delete, the S3Guard tables are updated
   * with deleted items. And only them.
   */
  @Test
  public void testProcessDeleteFailure() throws Throwable {
    String keyA = "/a/";
    String keyAB = "/a/b";
    String keyAC = "/a/c";
    Path pathA = qualifyKey(keyA);
    Path pathAB = qualifyKey(keyAB);
    Path pathAC = qualifyKey(keyAC);
    List<String> srcKeys = Lists.newArrayList(keyA, keyAB, keyAC);
    List<Path> src = Lists.newArrayList(pathA, pathAB, pathAC);
    List<DeleteObjectsRequest.KeyVersion> keyList = toDeleteRequests(srcKeys);
    List<Path> deleteForbidden = Lists.newArrayList(pathAB);
    final List<Path> deleteAllowed = Lists.newArrayList(pathA, pathAC);
    List<MultiObjectDeleteSupport.KeyPath> forbiddenKP =
        Lists.newArrayList(
            new MultiObjectDeleteSupport.KeyPath(keyAB, pathAB, true));
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        forbiddenKP);
    OperationTrackingStore store
        = new OperationTrackingStore();
    StoreContext storeContext = S3ATestUtils
            .createMockStoreContext(true, store, CONTEXT_ACCESSORS);
    MultiObjectDeleteSupport deleteSupport
        = new MultiObjectDeleteSupport(storeContext, null);
    List<Path> retainedMarkers = new ArrayList<>();
    Triple<List<Path>, List<Path>, List<Pair<Path, IOException>>>
        triple = deleteSupport.processDeleteFailure(ex,
        keyList,
        retainedMarkers);
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
    // because dir marker retention is on, we expect at least one retained
    // marker
    Assertions.assertThat(retainedMarkers).
        as("Retained Markers")
        .containsExactly(pathA);
    Assertions.assertThat(store.getDeleted()).
        as("List of tombstoned records")
        .doesNotContain(pathA);
  }


  private static final class MinimalContextAccessor
      implements ContextAccessors {

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

    @Override
    public ObjectMetadata getObjectMetadata(final String key)
        throws IOException {
      return new ObjectMetadata();
    }
  }

}
