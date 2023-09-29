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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DirEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestPrinter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.EntryFileIO;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.LoadedManifestData;
import org.apache.hadoop.util.functional.RemoteIterators;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_COMMITTER_CLASSNAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.EntryFileIO.toPath;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Support for committer tests.
 */
public final class ManifestCommitterTestSupport {

  private static final Logger LOG = LoggerFactory.getLogger(
      ManifestCommitterTestSupport.class);

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

  /**
   * Build directory property.
   * Value: {@value}.
   */
  public static final String PROJECT_BUILD_DIRECTORY_PROPERTY
      = "project.build.directory";

  /**
   * default number of task attempts for some tests.
   * Value: {@value}.
   */
  public static final int NUMBER_OF_TASK_ATTEMPTS = 2000;

  /**
   * Smaller number of task attempts for some tests against object
   * stores where IO overhead is higher.
   * Value: {@value}.
   */
  public static final int NUMBER_OF_TASK_ATTEMPTS_SMALL = 200;

  private ManifestCommitterTestSupport() {
  }

  /**
   * Create a random Job ID using the fork ID as part of the number if
   * set in the current process.
   * @return fork ID string in a format parseable by Jobs
   */
  public static String randomJobId() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id", "0001");
    int l = testUniqueForkId.length();
    String trailingDigits = testUniqueForkId.substring(l - 4, l);
    int digitValue;
    try {
      digitValue = Integer.valueOf(trailingDigits);
    } catch (NumberFormatException e) {
      digitValue = 0;
    }

    return String.format("%s%04d_%04d",
        FORMATTER.format(LocalDateTime.now()),
        (long) (Math.random() * 1000),
        digitValue);
  }

  public static File getProjectBuildDir() {
    String propval = System.getProperty(PROJECT_BUILD_DIRECTORY_PROPERTY);
    if (StringUtils.isEmpty(propval)) {
      propval = "target";
    }
    return new File(propval).getAbsoluteFile();
  }

  /**
   * Load a success file; fail if the file is empty/nonexistent.
   * @param fs filesystem
   * @param outputPath directory containing the success file.
   * @return the loaded file.
   * @throws IOException failure to find/load the file
   * @throws AssertionError file is 0-bytes long,
   */
  public static ManifestSuccessData loadSuccessFile(final FileSystem fs,
      final Path outputPath) throws IOException {
    Path success = new Path(outputPath, SUCCESS_MARKER);
    return ManifestSuccessData.load(fs, success);
  }

  /**
   * Load in the success data marker.
   * @param fs filesystem
   * @param outputDir ouptu path of job
   * @param minimumFileCount minimum number of files to have been created
   * @param jobId job ID, only verified if non-empty
   * @return the success data
   * @throws IOException IO failure
   */
  public static ManifestSuccessData validateSuccessFile(
      final FileSystem fs,
      final Path outputDir,
      final int minimumFileCount,
      final String jobId) throws IOException {
    Path successPath = new Path(outputDir, SUCCESS_MARKER);
    ManifestSuccessData successData
        = loadAndPrintSuccessData(fs, successPath);
    assertThat(successData.getCommitter())
        .describedAs("Committer field")
        .isEqualTo(MANIFEST_COMMITTER_CLASSNAME);
    assertThat(successData.getFilenames())
        .describedAs("Files committed")
        .hasSizeGreaterThanOrEqualTo(minimumFileCount);
    if (isNotEmpty(jobId)) {
      assertThat(successData.getJobId())
          .describedAs("JobID")
          .isEqualTo(jobId);
    }
    return successData;
  }

  /**
   * Load in and print a success data manifest.
   * @param fs filesystem
   * @param successPath full path to success file.
   * @return the success data
   * @throws IOException IO failure
   */
  public static ManifestSuccessData loadAndPrintSuccessData(
      FileSystem fs,
      Path successPath) throws IOException {
    LOG.info("Manifest {}", successPath);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    final ManifestPrinter showManifest = new ManifestPrinter(fs.getConf(), ps);
    ManifestSuccessData successData = showManifest.loadAndPrintManifest(fs, successPath);
    LOG.info("{}", baos);
    return successData;
  }

  /**
   * Validate all generated files from the manifest.
   * All files in the manifest must exist.
   * If the exclusive flag is set, only those must exist
   * (ignoring all temp files and everything in the _temporary
   * dir)
   * @param fs filesystem
   * @param destDir dest dir to scan
   * @param successData manifest
   * @param exclusive expect exclusive and complete data.
   * @return the files and their status
   * @throws IOException IO failure.
   */
  public static Map<Path, LocatedFileStatus> validateGeneratedFiles(
      FileSystem fs,
      Path destDir,
      ManifestSuccessData successData,
      boolean exclusive) throws IOException {
    Map<Path, LocatedFileStatus> fileListing = new HashMap<>();
    RemoteIterators.foreach(fs.listFiles(destDir, true),
        e -> {
          if (!e.getPath().getName().startsWith("_")) {
            fileListing.put(e.getPath(), e);
          }
        });
    final List<Path> actual = fileListing.keySet().stream()
        .sorted(Comparator.comparing(Path::getName))
        .collect(Collectors.toList());

    // map has all files other than temp ones and the success marker
    // what do we expect
    final List<Path> expected = filesInManifest(successData);
    expected.sort(Comparator.comparing(Path::getName));

    // all of those must be found
    Assertions.assertThat(actual)
        .describedAs("Files in FS expected to contain all listed in manifest")
        .containsAll(expected);

    // and if exclusive, that too
    if (exclusive) {
      Assertions.assertThat(actual)
          .describedAs("Files in FS expected to be exclusively of the job")
          .hasSize(expected.size())
          .containsExactlyInAnyOrderElementsOf(expected);
    }
    return fileListing;
  }

  /**
   * Given a manifest, get the list of filenames
   * and convert to paths.
   * @param successData data
   * @return the paths.
   */
  public static List<Path> filesInManifest(ManifestSuccessData successData) {
    return successData.getFilenames().stream()
        .map(AbstractManifestData::unmarshallPath)
        .collect(Collectors.toList());
  }

  /**
   * List a directory/directory tree.
   * @param fileSystem FS
   * @param path path
   * @param recursive do a recursive listing?
   * @return the number of files found.
   * @throws IOException failure.
   */
  public static long lsR(FileSystem fileSystem, Path path, boolean recursive)
      throws Exception {
    if (path == null) {
      // surfaces when someone calls getParent() on something at the top
      // of the path
      LOG.info("Empty path");
      return 0;
    }
    return RemoteIterators.foreach(fileSystem.listFiles(path, recursive),
        (status) -> LOG.info("{}", status));
  }

  /**
   * Assert that a file or dir entry matches the given parameters.
   * Matching on paths, not strings, helps validate marshalling.
   * @param fileOrDir file or directory
   * @param src source path
   * @param dest dest path
   * @param l length
   */
  static void assertFileEntryMatch(
      final FileEntry fileOrDir,
      final Path src,
      final Path dest,
      final long l) {
    String entry = fileOrDir.toString();
    assertThat(fileOrDir.getSourcePath())
        .describedAs("Source path of " + entry)
        .isEqualTo(src);
    assertThat(fileOrDir.getDestPath())
        .describedAs("Dest path of " + entry)
        .isEqualTo(dest);
    assertThat(fileOrDir.getSize())
        .describedAs("Size of " + entry)
        .isEqualTo(l);
  }

  /**
   * Assert that a dir entry matches the given parameters.
   * Matching on paths, not strings, helps validate marshalling.
   * @param fileOrDir file or directory
   * @param dest dest path
   * @param type type
   */
  static void assertDirEntryMatch(
      final DirEntry fileOrDir,
      final Path dest,
      final long type) {
    String entry = fileOrDir.toString();
    assertThat(fileOrDir.getDestPath())
        .describedAs("Dest path of " + entry)
        .isEqualTo(dest);
    assertThat(fileOrDir.getType())
        .describedAs("type of " + entry)
        .isEqualTo(type);
  }

  /**
   * Save a manifest to an entry file; returning the loaded manifest data.
   * Caller MUST clean up the temp file.
   * @param entryFileIO IO class
   * @param manifest manifest to process.
   * @return info about the load
   * @throws IOException write failure
   */
  public static LoadedManifestData saveManifest(EntryFileIO entryFileIO, TaskManifest manifest)
      throws IOException {
    final File tempFile = File.createTempFile("entries", ".seq");
    final SequenceFile.Writer writer = entryFileIO.createWriter(tempFile);
    return new LoadedManifestData(
        manifest.getDestDirectories(),
        toPath(tempFile),
        EntryFileIO.write(writer, manifest.getFilesToCommit(), true));
  }

  /**
   * Closeable which can be used to safely close writers in
   * a try-with-resources block..
   */
  public static final class CloseWriter<K, V> implements AutoCloseable {

    private final RecordWriter<K, V> writer;

    private final TaskAttemptContext context;

    public CloseWriter(RecordWriter<K, V> writer,
        TaskAttemptContext context) {
      this.writer = writer;
      this.context = context;
    }

    @Override
    public void close() {
      try {
        writer.close(context);
      } catch (IOException | InterruptedException e) {
        LOG.error("When closing {} on context {}",
            writer, context, e);
      }
    }
  }

  public static final String ATTEMPT_STRING =
      "attempt_%s_m_%06d_%d";

  /**
   * Creates a random JobID and then as many tasks
   * with the specific number of task attempts.
   */
  public static final class JobAndTaskIDsForTests {

    /** Job ID; will be created uniquely for each instance. */
    private final String jobId;

    /**
     * Store the details as strings; generate
     * IDs on demand.
     */
    private final String[][] taskAttempts;

    /**
     * Constructor.
     * @param tasks number of tasks.
     * @param attempts number of attempts.
     */
    public JobAndTaskIDsForTests(int tasks, int attempts) {
      this(randomJobId(), tasks, attempts);
    }

    public JobAndTaskIDsForTests(final String jobId,
        int tasks, int attempts) {
      this.jobId = jobId;
      this.taskAttempts = new String[tasks][attempts];
      for (int i = 0; i < tasks; i++) {
        for (int j = 0; j < attempts; j++) {
          String a = String.format(ATTEMPT_STRING,
              jobId, i, j);
          this.taskAttempts[i][j] = a;
        }
      }
    }

    /**
     * Get the job ID.
     * @return job ID string.
     */
    public String getJobId() {
      return jobId;
    }

    /**
     * Get the job ID as the MR type.
     * @return job ID type.
     */
    public JobID getJobIdType() {
      return getTaskIdType(0).getJobID();
    }

    /**
     * Get a task attempt ID.
     * @param task task index
     * @param attempt attempt number.
     * @return the task attempt.
     */
    public String getTaskAttempt(int task, int attempt) {
      return taskAttempts[task][attempt];
    }

    /**
     * Get task attempt ID as the MR type.
     * @param task task index
     * @param attempt attempt number.
     * @return the task attempt type
     */
    public TaskAttemptID getTaskAttemptIdType(int task, int attempt) {
      return TaskAttemptID.forName(getTaskAttempt(task, attempt));
    }

    /**
     * Get task ID as the MR type.
     * @param task task index
     * @return the task ID type
     */
    public TaskID getTaskIdType(int task) {
      return TaskAttemptID.forName(getTaskAttempt(task, 0)).getTaskID();
    }

    /**
     * Get task ID as a string.
     * @param task task index
     * @return the task ID
     */
    public String getTaskId(int task) {
      return getTaskIdType(task).toString();
    }

  }
}
