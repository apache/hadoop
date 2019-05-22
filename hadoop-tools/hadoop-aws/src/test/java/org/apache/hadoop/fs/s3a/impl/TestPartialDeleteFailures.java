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

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AInputPolicy;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.S3AStorageStatistics;
import org.apache.hadoop.fs.s3a.s3guard.BulkOperationState;
import org.apache.hadoop.fs.s3a.s3guard.DirListingMetadata;
import org.apache.hadoop.fs.s3a.s3guard.MetadataStore;
import org.apache.hadoop.fs.s3a.s3guard.PathMetadata;
import org.apache.hadoop.fs.s3a.s3guard.RenameTracker;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;

import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.ACCESS_DENIED;
import static org.apache.hadoop.fs.s3a.impl.MultiObjectDeleteSupport.removeUndeletedPaths;
import static org.junit.Assert.assertEquals;

/**
 * Unit test suite covering translation of AWS SDK exceptions to S3A exceptions,
 * and retry/recovery policies.
 */
public class TestPartialDeleteFailures {

  private StoreContext context;

  private static Path qualify(String k) {
    return new Path("s3a://bucket/" + k);
  }

  @Before
  public void setUp() throws Exception {
    context = craateMockStoreContext(true,
        new OperationTrackingStore());
  }

  @Test
  public void testDeleteExtraction() {
    List<Path> src = pathList("a", "a/b", "a/c");
    List<Path> rejected = pathList("a/b");
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        rejected);
    List<Path> undeleted = removeUndeletedPaths(ex, src,
        TestPartialDeleteFailures::qualify);
    assertEquals(rejected, undeleted);
  }

  @Test
  public void testSplitKeysFromResults() throws Throwable {
    List<Path> src = pathList("a", "a/b", "a/c");
    List<Path> rejected = pathList("a/b");
    List<DeleteObjectsRequest.KeyVersion> keys = keysToDelete(src);
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        rejected);
    Pair<List<Path>, List<Path>> pair =
        new MultiObjectDeleteSupport(context)
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
        .map(TestPartialDeleteFailures::qualify)
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
    Path pathA = qualify("/a");
    Path pathAB = qualify("/a/b");
    Path pathAC = qualify("/a/c");
    List<Path> src = Lists.newArrayList(pathA, pathAB, pathAC);
    List<DeleteObjectsRequest.KeyVersion> keyList = keysToDelete(src);
    List<Path> deleteForbidden = Lists.newArrayList(pathAB);
    final List<Path> deleteAllowed = Lists.newArrayList(pathA, pathAC);
    MultiObjectDeleteException ex = createDeleteException(ACCESS_DENIED,
        deleteForbidden);
    OperationTrackingStore store
        = new OperationTrackingStore();
    StoreContext storeContext = craateMockStoreContext(true, store);
    MultiObjectDeleteSupport deleteSupport
        = new MultiObjectDeleteSupport(storeContext);
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


  StoreContext craateMockStoreContext(boolean multiDelete,
      OperationTrackingStore store) throws URISyntaxException, IOException {
    URI name = new URI("s3a://bucket");
    return new StoreContext(
        name,
        "bucket",
        new Configuration(),
        "alice",
        UserGroupInformation.getCurrentUser(),
        BlockingThreadPoolExecutorService.newInstance(
            4,
            4,
            10, TimeUnit.SECONDS,
            "s3a-transfer-shared"),
        Constants.DEFAULT_EXECUTOR_CAPACITY,
        new Invoker(RetryPolicies.TRY_ONCE_THEN_FAIL, Invoker.LOG_EVENT),
        new S3AInstrumentation(name),
        new S3AStorageStatistics(),
        S3AInputPolicy.Normal,
        ChangeDetectionPolicy.createPolicy(ChangeDetectionPolicy.Mode.None,
            ChangeDetectionPolicy.Source.ETag, false),
        multiDelete,
        store,
        TestPartialDeleteFailures::qualify,
        false,
        false,
        null);
  }

  /**
   * MetadataStore which tracks what is deleted and added.
   */
  private static class OperationTrackingStore implements MetadataStore {

    private final List<Path> deleted = new ArrayList<>();

    private final List<Path> created = new ArrayList<>();

    @Override
    public void initialize(final FileSystem fs) {
    }

    @Override
    public void initialize(final Configuration conf) {
    }

    @Override
    public void delete(final Path path) {
      deleted.add(path);
    }

    @Override
    public void forgetMetadata(final Path path) {

    }

    @Override
    public void deleteSubtree(final Path path) {

    }

    @Override
    public PathMetadata get(final Path path) {
      return null;
    }

    @Override
    public PathMetadata get(final Path path,
        final boolean wantEmptyDirectoryFlag) {
      return null;
    }

    @Override
    public DirListingMetadata listChildren(final Path path) {
      return null;
    }

    @Override
    public void move(
        @Nullable final Collection<Path> pathsToDelete,
        @Nullable final Collection<PathMetadata> pathsToCreate,
        @Nullable final BulkOperationState moveState) {

    }

    @Override
    public void put(final PathMetadata meta) throws IOException {
      put(meta, null);
    }

    @Override
    public void put(final PathMetadata meta,
        final BulkOperationState operationState) {
      created.add(meta.getFileStatus().getPath());
    }

    @Override
    public void put(final Collection<PathMetadata> metas,
        final BulkOperationState operationState) {
      metas.stream().forEach(meta -> put(meta, null));
    }

    @Override
    public void put(final DirListingMetadata meta) {
      created.add(meta.getPath());
    }

    @Override
    public void destroy() {

    }

    @Override
    public void prune(final long modTime)
        throws IOException, UnsupportedOperationException {

    }

    @Override
    public void prune(final long modTime, final String keyPrefix)
        throws IOException, UnsupportedOperationException {
    }

    @Override
    public Map<String, String> getDiagnostics() {
      return null;
    }

    @Override
    public void updateParameters(final Map<String, String> parameters) {
    }

    @Override
    public void close() {
    }

    public List<Path> getDeleted() {
      return deleted;
    }

    public List<Path> getCreated() {
      return created;
    }

    @Override
    public RenameTracker initiateRenameOperation(
        final StoreContext storeContext,
        final Path source,
        final FileStatus sourceStatus,
        final Path dest) throws IOException {
      throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public void addAncestors(final Path qualifiedPath,
        @Nullable final BulkOperationState operationState) throws IOException {

    }
  }

}
