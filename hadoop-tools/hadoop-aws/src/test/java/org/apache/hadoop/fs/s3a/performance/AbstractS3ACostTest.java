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

package org.apache.hadoop.fs.s3a.performance;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.assertj.core.api.Assertions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AbstractS3ATestBase;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.fs.s3a.impl.DirectoryPolicy;
import org.apache.hadoop.fs.s3a.impl.InternalConstants;
import org.apache.hadoop.fs.s3a.impl.StatusProbeEnum;
import org.apache.hadoop.fs.s3a.statistics.StatisticTypeEnum;
import org.apache.hadoop.fs.store.audit.AuditSpan;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_BULK_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_DELETE_REQUEST;
import static org.apache.hadoop.fs.s3a.performance.OperationCost.*;
import static org.apache.hadoop.fs.s3a.performance.OperationCostValidator.expect;
import static org.apache.hadoop.fs.s3a.performance.OperationCostValidator.probe;
import static org.apache.hadoop.test.AssertExtensions.dynamicDescription;

/**
 * Abstract class for tests which make assertions about cost.
 * <p></p>
 * Factored out from {@code ITestS3AFileOperationCost}
 */
public class AbstractS3ACostTest extends AbstractS3ATestBase {

  /**
   * Parameter: should directory markers be retained?
   */
  private final boolean keepMarkers;

  private boolean isKeeping;

  private boolean isDeleting;

  private OperationCostValidator costValidator;

  /**
   * Is bulk deletion enabled?
   */
  private boolean isBulkDelete;

  /**
   * Which statistic measures marker deletion?
   * this is the bulk delete statistic by default;
   * if that is disabled it becomes the single delete counter.
   */
  private Statistic deleteMarkerStatistic;


  /**
   * Constructor for parameterized tests.
   * @param keepMarkers should markers be tested.
   */
  protected AbstractS3ACostTest(
      final boolean keepMarkers) {
    this.keepMarkers = keepMarkers;
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    String bucketName = getTestBucketName(conf);
    String arnKey = String.format(InternalConstants.ARN_BUCKET_OPTION, bucketName);
    String arn = conf.getTrimmed(arnKey, "");

    removeBaseAndBucketOverrides(bucketName, conf,
        DIRECTORY_MARKER_POLICY,
        AUTHORITATIVE_PATH);
    // directory marker options
    conf.set(DIRECTORY_MARKER_POLICY,
        keepMarkers
            ? DIRECTORY_MARKER_POLICY_KEEP
            : DIRECTORY_MARKER_POLICY_DELETE);
    disableFilesystemCaching(conf);

    // AccessPoint ARN is the only per bucket configuration that must be kept.
    if (!arn.isEmpty()) {
      conf.set(arnKey, arn);
    }

    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    S3AFileSystem fs = getFileSystem();
    isKeeping = isKeepingMarkers();

    isDeleting = !isKeeping;

    // check that the FS has the expected state
    DirectoryPolicy markerPolicy = fs.getDirectoryMarkerPolicy();
    Assertions.assertThat(markerPolicy.getMarkerPolicy())
        .describedAs("Marker policy for filesystem %s", fs)
        .isEqualTo(isKeepingMarkers()
            ? DirectoryPolicy.MarkerPolicy.Keep
            : DirectoryPolicy.MarkerPolicy.Delete);
    setupCostValidator();

    // determine bulk delete settings
    isBulkDelete = isBulkDeleteEnabled(getFileSystem());
    deleteMarkerStatistic = isBulkDelete()
        ? OBJECT_BULK_DELETE_REQUEST
        : OBJECT_DELETE_REQUEST;

    setSpanSource(fs);
  }

  protected void setupCostValidator() {
    // All counter statistics of the filesystem are added as metrics.
    // Durations too, as they have counters of success and failure.
    OperationCostValidator.Builder builder = OperationCostValidator.builder(
        getFileSystem());
    EnumSet.allOf(Statistic.class).stream()
        .filter(s ->
            s.getType() == StatisticTypeEnum.TYPE_COUNTER
                || s.getType() == StatisticTypeEnum.TYPE_DURATION)
        .forEach(s -> builder.withMetric(s));
    costValidator = builder.build();
  }

  public boolean isDeleting() {
    return isDeleting;
  }

  public boolean isKeepingMarkers() {
    return keepMarkers;
  }

  /**
   * A special object whose toString() value is the current
   * state of the metrics.
   */
  protected Object getMetricSummary() {
    return costValidator;
  }

  /**
   * Create then close the file through the builder API.
   * @param path path
   * @param overwrite overwrite flag
   * @param recursive true == skip parent existence check
   * @param cost expected cost
   * @return path to new object.
   */
  protected Path buildFile(Path path,
      boolean overwrite,
      boolean recursive,
      OperationCost cost) throws Exception {
    resetStatistics();
    verify(cost, () -> {
      FSDataOutputStreamBuilder builder = getFileSystem().createFile(path)
          .overwrite(overwrite);
      if (recursive) {
        builder.recursive();
      }
      FSDataOutputStream stream = builder.build();
      stream.close();
      return stream.toString();
    });
    return path;
  }

  /**
   * Create a directory, returning its path.
   * @param p path to dir.
   * @return path of new dir
   */
  protected Path dir(Path p) throws IOException {
    mkdirs(p);
    return p;
  }

  /**
   * Create a file, returning its path.
   * @param p path to file.
   * @return path of new file
   */
  protected Path file(Path p) throws IOException {
    return file(p, true);
  }

  /**
   * Create a file, returning its path.
   * @param path path to file.
   * @param overwrite overwrite flag
   * @return path of new file
   */
  protected Path file(Path path, final boolean overwrite)
      throws IOException {
    getFileSystem().create(path, overwrite).close();
    return path;
  }

  /**
   * Touch a file, overwriting.
   * @param path path
   * @return path to new object.
   */
  protected Path create(Path path) throws Exception {
    return create(path, true, CREATE_FILE_OVERWRITE);
  }

  /**
   * Create then close the file.
   * @param path path
   * @param overwrite overwrite flag
   * @param cost expected cost

   * @return path to new object.
   */
  protected Path create(Path path, boolean overwrite,
      OperationCost cost) throws Exception {
    return verify(cost, () ->
        file(path, overwrite));
  }

  /**
   * Execute rename, returning the current metrics.
   * For use in l-expressions.
   * @param source source path.
   * @param dest dest path
   * @return a string for exceptions.
   */
  public String execRename(final Path source,
      final Path dest) throws IOException {
    getFileSystem().rename(source, dest);
    return String.format("rename(%s, %s): %s",
        dest, source, getMetricSummary());
  }

  /**
   * How many directories are in a path?
   * @param path path to probe.
   * @return the number of entries below root this path is
   */
  protected int directoriesInPath(Path path) {
    return path.isRoot() ? 0 : 1 + directoriesInPath(path.getParent());
  }

  /**
   * Reset all the metrics being tracked.
   */
  private void resetStatistics() {
    costValidator.resetMetricDiffs();
  }

  /**
   * Execute a closure and verify the metrics.
   * @param eval closure to evaluate
   * @param expected varargs list of expected diffs
   * @param <T> return type.
   * @return the result of the evaluation
   */
  protected <T> T verifyMetrics(
      Callable<T> eval,
      OperationCostValidator.ExpectedProbe... expected) throws Exception {
    span();
    return costValidator.exec(eval, expected);

  }

  /**
   * Execute a closure, expecting an exception.
   * Verify the metrics after the exception has been caught and
   * validated.
   * @param clazz type of exception
   * @param text text to look for in exception (optional)
   * @param eval closure to evaluate
   * @param expected varargs list of expected diffs
   * @param <T> return type of closure
   * @param <E> exception type
   * @return the exception caught.
   * @throws Exception any other exception
   */
  protected <T, E extends Throwable> E verifyMetricsIntercepting(
      Class<E> clazz,
      String text,
      Callable<T> eval,
      OperationCostValidator.ExpectedProbe... expected) throws Exception {
    span();
    return costValidator.intercepting(clazz, text, eval, expected);
  }

  /**
   * Execute a closure expecting an exception.
   * @param clazz type of exception
   * @param text text to look for in exception (optional)
   * @param cost expected cost declaration.
   * @param eval closure to evaluate
   * @param <T> return type of closure
   * @param <E> exception type
   * @return the exception caught.
   * @throws Exception any other exception
   */
  protected <T, E extends Throwable> E interceptOperation(
      Class<E> clazz,
      String text,
      OperationCost cost,
      Callable<T> eval) throws Exception {
    return verifyMetricsIntercepting(clazz, text, eval, always(cost));
  }

  /**
   * Declare the expected cost on any FS.
   * @param cost costs to expect
   * @return a probe.
   */
  protected OperationCostValidator.ExpectedProbe always(
      OperationCost cost) {
    return expect(true, cost);
  }

  /**
   * A metric diff which must hold when the fs is keeping markers.
   * @param cost expected cost
   * @return the diff.
   */
  protected OperationCostValidator.ExpectedProbe whenKeeping(
      OperationCost cost) {
    return expect(isKeepingMarkers(), cost);
  }

  /**
   * A metric diff which must hold when the fs is keeping markers.
   * @param cost expected cost
   * @return the diff.
   */
  protected OperationCostValidator.ExpectedProbe whenDeleting(
      OperationCost cost) {
    return expect(isDeleting(), cost);
  }

  /**
   * Execute a closure expecting a specific number of HEAD/LIST calls.
   * The operation is always evaluated.
   * A span is always created prior to the invocation; saves trouble
   * in tests that way.
   * @param cost expected cost
   * @param eval closure to evaluate
   * @param <T> return type of closure
   * @return the result of the evaluation
   */
  protected <T> T verify(
      OperationCost cost,
      Callable<T> eval) throws Exception {
    return verifyMetrics(eval,
        always(cost), OperationCostValidator.always());
  }

  /**
   * Execute {@code S3AFileSystem#innerGetFileStatus(Path, boolean, Set)}
   * for the given probes.
   * expect the specific HEAD/LIST count.
   * @param path path
   * @param needEmptyDirectoryFlag look for empty directory
   * @param probes file status probes to perform
   * @param cost expected cost
   * @return the status
   */
  public S3AFileStatus verifyInnerGetFileStatus(
      Path path,
      boolean needEmptyDirectoryFlag,
      Set<StatusProbeEnum> probes,
      OperationCost cost) throws Exception {
    return verify(cost, () ->
        innerGetFileStatus(getFileSystem(),
            path,
            needEmptyDirectoryFlag,
            probes));
  }

  /**
   * Execute {@code S3AFileSystem#innerGetFileStatus(Path, boolean, Set)}
   * for the given probes -expect a FileNotFoundException,
   * and the specific HEAD/LIST count.
   * @param path path
   * @param needEmptyDirectoryFlag look for empty directory
   * @param probes file status probes to perform
   * @param cost expected cost
   */

  public void interceptGetFileStatusFNFE(
      Path path,
      boolean needEmptyDirectoryFlag,
      Set<StatusProbeEnum> probes,
      OperationCost cost) throws Exception {
    try (AuditSpan span = span()) {
      interceptOperation(FileNotFoundException.class, "",
          cost, () ->
              innerGetFileStatus(getFileSystem(),
                  path,
                  needEmptyDirectoryFlag,
                  probes));
    }
  }

  /**
   * Probe for a path being a directory.
   * @param path path
   * @param expected expected outcome
   * @param cost expected cost
   */
  protected void isDir(Path path,
      boolean expected,
      OperationCost cost) throws Exception {
    boolean b = verify(cost, () ->
        getFileSystem().isDirectory(path));
    Assertions.assertThat(b)
        .describedAs("isDirectory(%s)", path)
        .isEqualTo(expected);
  }

  /**
   * Probe for a path being a file.
   * @param path path
   * @param expected expected outcome
   * @param cost expected cost
   */
  protected void isFile(Path path,
      boolean expected,
      OperationCost cost) throws Exception {
    boolean b = verify(cost, () ->
        getFileSystem().isFile(path));
    Assertions.assertThat(b)
        .describedAs("isFile(%s)", path)
        .isEqualTo(expected);
  }

  /**
   * A metric diff which must always hold.
   * @param stat metric source
   * @param expected expected value.
   * @return the diff.
   */
  protected OperationCostValidator.ExpectedProbe with(
      final Statistic stat, final int expected) {
    return probe(stat, expected);
  }

  /**
   * A metric diff which must hold when the fs is keeping markers.
   * @param stat metric source
   * @param expected expected value.
   * @return the diff.
   */
  protected OperationCostValidator.ExpectedProbe withWhenKeeping(
      final Statistic stat,
      final int expected) {
    return probe(isKeepingMarkers(), stat, expected);
  }

  /**
   * A metric diff which must hold when the fs is keeping markers.
   * @param stat metric source
   * @param expected expected value.
   * @return the diff.
   */
  protected OperationCostValidator.ExpectedProbe withWhenDeleting(
      final Statistic stat,
      final int expected) {
    return probe(isDeleting(), stat, expected);
  }

  /**
   * Assert the empty directory status of a file is as expected.
   * The raised assertion message includes a list of the path.
   * @param status status to probe.
   * @param expected expected value
   */
  protected void assertEmptyDirStatus(final S3AFileStatus status,
      final Tristate expected) {
    Assertions.assertThat(status.isEmptyDirectory())
        .describedAs(dynamicDescription(() ->
            "FileStatus says directory is not empty: " + status
                + "\n" + ContractTestUtils.ls(
                    getFileSystem(), status.getPath())))
        .isEqualTo(expected);
  }

  /**
   * Is bulk deletion enabled?
   */
  protected boolean isBulkDelete() {
    return isBulkDelete;
  }

  /**
   * Which statistic measures marker deletion?
   * this is the bulk delete statistic by default;
   * if that is disabled it becomes the single delete counter.
   */
  protected Statistic getDeleteMarkerStatistic() {
    return deleteMarkerStatistic;
  }
}
