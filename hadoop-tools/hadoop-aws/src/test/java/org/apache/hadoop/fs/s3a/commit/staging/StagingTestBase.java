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

package org.apache.hadoop.fs.s3a.commit.staging;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.MockS3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.AbstractCommitITest;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;
import org.apache.hadoop.fs.s3a.commit.MiniDFSClusterService;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.test.HadoopTestBase;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test base for mock tests of staging committers:
 * core constants and static methods, inner classes
 * for specific test types.
 *
 * Some of the verification methods here are unused...they are being left
 * in place in case changes on the implementation make the verifications
 * relevant again.
 */
public class StagingTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(StagingTestBase.class);

  public static final String BUCKET = MockS3AFileSystem.BUCKET;
  public static final String OUTPUT_PREFIX = "output/path";
  /** The raw bucket URI Path before any canonicalization. */
  public static final Path RAW_BUCKET_PATH =
      new Path("s3a://" + BUCKET + "/");
  /** The raw bucket URI Path before any canonicalization. */
  public static final URI RAW_BUCKET_URI =
      RAW_BUCKET_PATH.toUri();
  public static Path outputPath =
      new Path("s3a://" + BUCKET + "/" + OUTPUT_PREFIX);
  public static URI outputPathUri = outputPath.toUri();
  public static Path root;

  protected StagingTestBase() {
  }

  /**
   * Sets up the mock filesystem instance and binds it to the
   * {@link FileSystem#get(URI, Configuration)} call for the supplied URI
   * and config.
   * All standard mocking setup MUST go here.
   * @param conf config to use
   * @param outcome tuple of outcomes to store in mock FS
   * @return the filesystem created
   * @throws IOException IO problems.
   */
  protected static S3AFileSystem createAndBindMockFSInstance(Configuration conf,
      Pair<StagingTestBase.ClientResults, StagingTestBase.ClientErrors> outcome)
      throws IOException {
    S3AFileSystem mockFs = mockS3AFileSystemRobustly();
    MockS3AFileSystem wrapperFS = new MockS3AFileSystem(mockFs, outcome);
    URI uri = RAW_BUCKET_URI;
    wrapperFS.initialize(uri, conf);
    root = wrapperFS.makeQualified(new Path("/"));
    outputPath = new Path(root, OUTPUT_PREFIX);
    outputPathUri = outputPath.toUri();
    FileSystemTestHelper.addFileSystemForTesting(uri, conf, wrapperFS);
    return mockFs;
  }

  private static S3AFileSystem mockS3AFileSystemRobustly() {
    S3AFileSystem mockFS = mock(S3AFileSystem.class);
    doNothing().when(mockFS).incrementReadOperations();
    doNothing().when(mockFS).incrementWriteOperations();
    doNothing().when(mockFS).incrementWriteOperations();
    doNothing().when(mockFS).incrementWriteOperations();
    return mockFS;
  }

  /**
   * Look up the FS by URI, return a (cast) Mock wrapper.
   * @param conf config
   * @return the FS
   * @throws IOException IO Failure
   */
  public static MockS3AFileSystem lookupWrapperFS(Configuration conf)
      throws IOException {
    return (MockS3AFileSystem) FileSystem.get(outputPathUri, conf);
  }

  public static void verifyCompletion(FileSystem mockS3) throws IOException {
    verifyCleanupTempFiles(mockS3);
    verifyNoMoreInteractions(mockS3);
  }

  public static void verifyDeleted(FileSystem mockS3, Path path)
      throws IOException {
    verify(mockS3).delete(path, true);
  }

  public static void verifyDeleted(FileSystem mockS3, String child)
      throws IOException {
    verifyDeleted(mockS3, new Path(outputPath, child));
  }

  public static void verifyCleanupTempFiles(FileSystem mockS3)
      throws IOException {
    verifyDeleted(mockS3,
        new Path(outputPath, CommitConstants.TEMPORARY));
  }

  protected static void assertConflictResolution(
      StagingCommitter committer,
      JobContext job,
      ConflictResolution mode) {
    Assert.assertEquals("Conflict resolution mode in " + committer,
        mode, committer.getConflictResolutionMode(job, new Configuration()));
  }

  public static void pathsExist(FileSystem mockS3, String... children)
      throws IOException {
    for (String child : children) {
      pathExists(mockS3, new Path(outputPath, child));
    }
  }

  public static void pathExists(FileSystem mockS3, Path path)
      throws IOException {
    when(mockS3.exists(path)).thenReturn(true);
  }

  public static void pathDoesNotExist(FileSystem mockS3, Path path)
      throws IOException {
    when(mockS3.exists(path)).thenReturn(false);
  }

  public static void canDelete(FileSystem mockS3, String... children)
      throws IOException {
    for (String child : children) {
      canDelete(mockS3, new Path(outputPath, child));
    }
  }

  public static void canDelete(FileSystem mockS3, Path f) throws IOException {
    when(mockS3.delete(f,
        true /* recursive */))
        .thenReturn(true);
  }

  public static void verifyExistenceChecked(FileSystem mockS3, String child)
      throws IOException {
    verifyExistenceChecked(mockS3, new Path(outputPath, child));
  }

  public static void verifyExistenceChecked(FileSystem mockS3, Path path)
      throws IOException {
    verify(mockS3).exists(path);
  }

  /**
   * Provides setup/teardown of a MiniDFSCluster for tests that need one.
   */
  public static class MiniDFSTest extends HadoopTestBase {

    private static MiniDFSClusterService hdfs;

    private static JobConf conf = null;

    protected static JobConf getConfiguration() {
      return conf;
    }

    protected static FileSystem getDFS() {
      return hdfs.getClusterFS();
    }

    /**
     * Setup the mini HDFS cluster.
     * @throws IOException Failure
     */
    @BeforeClass
    @SuppressWarnings("deprecation")
    public static void setupHDFS() throws IOException {
      if (hdfs == null) {
        JobConf c = new JobConf();
        hdfs = new MiniDFSClusterService();
        hdfs.init(c);
        hdfs.start();
        conf = c;
      }
    }

    @SuppressWarnings("ThrowableNotThrown")
    @AfterClass
    public static void teardownFS() throws IOException {
      ServiceOperations.stopQuietly(hdfs);
      conf = null;
      hdfs = null;
    }

  }

  /**
   * Base class for job committer tests.
   * @param <C> committer
   */
  public abstract static class JobCommitterTest<C extends OutputCommitter>
      extends HadoopTestBase {
    private static final JobID JOB_ID = new JobID("job", 1);
    private JobConf jobConf;

    // created in BeforeClass
    private S3AFileSystem mockFS = null;
    private MockS3AFileSystem wrapperFS = null;
    private JobContext job = null;

    // created in Before
    private StagingTestBase.ClientResults results = null;
    private StagingTestBase.ClientErrors errors = null;
    private AmazonS3 mockClient = null;

    @Before
    public void setupJob() throws Exception {
      this.jobConf = new JobConf();
      jobConf.set(InternalCommitterConstants.FS_S3A_COMMITTER_STAGING_UUID,
          UUID.randomUUID().toString());
      jobConf.setBoolean(
          CommitConstants.CREATE_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
          false);

      this.job = new JobContextImpl(jobConf, JOB_ID);
      this.results = new StagingTestBase.ClientResults();
      this.errors = new StagingTestBase.ClientErrors();
      this.mockClient = newMockS3Client(results, errors);
      this.mockFS = createAndBindMockFSInstance(jobConf,
          Pair.of(results, errors));
      this.wrapperFS = lookupWrapperFS(jobConf);
      // and bind the FS
      wrapperFS.setAmazonS3Client(mockClient);
    }

    public S3AFileSystem getMockS3A() {
      return mockFS;
    }

    public MockS3AFileSystem getWrapperFS() {
      return wrapperFS;
    }

    public JobContext getJob() {
      return job;
    }

    /**
     * Create a task attempt for a job by creating a stub task ID.
     * @return a task attempt
     */
    public TaskAttemptContext createTaskAttemptForJob() {
      return AbstractCommitITest.taskAttemptForJob(
          MRBuilderUtils.newJobId(1, JOB_ID.getId(), 1), job);
    }

    protected StagingTestBase.ClientResults getMockResults() {
      return results;
    }

    protected StagingTestBase.ClientErrors getMockErrors() {
      return errors;
    }

    abstract C newJobCommitter() throws Exception;
  }

  /** Abstract test of task commits. */
  public abstract static class TaskCommitterTest<C extends OutputCommitter>
      extends JobCommitterTest<C> {
    private static final TaskAttemptID AID = new TaskAttemptID(
        new TaskID(JobCommitterTest.JOB_ID, TaskType.REDUCE, 2), 3);

    private C jobCommitter = null;
    private TaskAttemptContext tac = null;
    private File tempDir;

    @Before
    public void setupTask() throws Exception {
      this.jobCommitter = newJobCommitter();
      jobCommitter.setupJob(getJob());

      this.tac = new TaskAttemptContextImpl(
          new Configuration(getJob().getConfiguration()), AID);

      // get the task's configuration copy so modifications take effect
      String tmp = System.getProperty(
          StagingCommitterConstants.JAVA_IO_TMPDIR);
      tempDir = new File(tmp);
      tac.getConfiguration().set(Constants.BUFFER_DIR, tmp + "/buffer");
      tac.getConfiguration().set(
          CommitConstants.FS_S3A_COMMITTER_STAGING_TMP_PATH,
          tmp + "/cluster");
    }

    protected C getJobCommitter() {
      return jobCommitter;
    }

    protected TaskAttemptContext getTAC() {
      return tac;
    }

    abstract C newTaskCommitter() throws Exception;

    protected File getTempDir() {
      return tempDir;
    }
  }

  /**
   * Results accrued during mock runs.
   * This data is serialized in MR Tests and read back in in the test runner
   */
  public static class ClientResults implements Serializable {
    private static final long serialVersionUID = -3137637327090709905L;
    // For inspection of what the committer did
    private final Map<String, InitiateMultipartUploadRequest> requests =
        Maps.newHashMap();
    private final List<String> uploads = Lists.newArrayList();
    private final List<UploadPartRequest> parts = Lists.newArrayList();
    private final Map<String, List<String>> tagsByUpload = Maps.newHashMap();
    private final List<CompleteMultipartUploadRequest> commits =
        Lists.newArrayList();
    private final List<AbortMultipartUploadRequest> aborts
        = Lists.newArrayList();
    private final Map<String, String> activeUploads =
        Maps.newHashMap();
    private final List<DeleteObjectRequest> deletes = Lists.newArrayList();

    public Map<String, InitiateMultipartUploadRequest> getRequests() {
      return requests;
    }

    public List<String> getUploads() {
      return uploads;
    }

    public List<UploadPartRequest> getParts() {
      return parts;
    }

    public Map<String, List<String>> getTagsByUpload() {
      return tagsByUpload;
    }

    public List<CompleteMultipartUploadRequest> getCommits() {
      return commits;
    }

    public List<AbortMultipartUploadRequest> getAborts() {
      return aborts;
    }

    public List<DeleteObjectRequest> getDeletes() {
      return deletes;
    }

    public void resetDeletes() {
      deletes.clear();
    }

    public void resetUploads() {
      uploads.clear();
      activeUploads.clear();
    }

    public void resetCommits() {
      commits.clear();
    }

    public void resetRequests() {
      requests.clear();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          super.toString());
      sb.append("{ requests=").append(requests.size());
      sb.append(", uploads=").append(uploads.size());
      sb.append(", parts=").append(parts.size());
      sb.append(", tagsByUpload=").append(tagsByUpload.size());
      sb.append(", commits=").append(commits.size());
      sb.append(", aborts=").append(aborts.size());
      sb.append(", deletes=").append(deletes.size());
      sb.append('}');
      return sb.toString();
    }
  }

  /** Control errors to raise in mock S3 client. */
  public static class ClientErrors {
    // For injecting errors
    private int failOnInit = -1;
    private int failOnUpload = -1;
    private int failOnCommit = -1;
    private int failOnAbort = -1;
    private boolean recover = false;

    public void failOnInit(int initNum) {
      this.failOnInit = initNum;
    }

    public void failOnUpload(int uploadNum) {
      this.failOnUpload = uploadNum;
    }

    public void failOnCommit(int commitNum) {
      this.failOnCommit = commitNum;
    }

    public void failOnAbort(int abortNum) {
      this.failOnAbort = abortNum;
    }

    public void recoverAfterFailure() {
      this.recover = true;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(
          "ClientErrors{");
      sb.append("failOnInit=").append(failOnInit);
      sb.append(", failOnUpload=").append(failOnUpload);
      sb.append(", failOnCommit=").append(failOnCommit);
      sb.append(", failOnAbort=").append(failOnAbort);
      sb.append(", recover=").append(recover);
      sb.append('}');
      return sb.toString();
    }

    public int getFailOnInit() {
      return failOnInit;
    }

    public int getFailOnUpload() {
      return failOnUpload;
    }

    public int getFailOnCommit() {
      return failOnCommit;
    }

    public int getFailOnAbort() {
      return failOnAbort;
    }

    public boolean isRecover() {
      return recover;
    }
  }

  /**
   * InvocationOnMock.getArgumentAt comes and goes with Mockito versions; this
   * helper method is designed to be resilient to change.
   * @param invocation invocation to query
   * @param index argument index
   * @param clazz class of return type
   * @param <T> type of return
   * @return the argument of the invocation, cast to the given type.
   */
  @SuppressWarnings("unchecked")
  private static<T> T getArgumentAt(InvocationOnMock invocation, int index,
      Class<T> clazz) {
    return (T)invocation.getArguments()[index];
  }

  /**
   * Instantiate mock client with the results and errors requested.
   * @param results results to accrue
   * @param errors when (if any) to fail
   * @return the mock client to patch in to a committer/FS instance
   */
  public static AmazonS3 newMockS3Client(final ClientResults results,
      final ClientErrors errors) {
    AmazonS3Client mockClient = mock(AmazonS3Client.class);
    final Object lock = new Object();

    // initiateMultipartUpload
    when(mockClient
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class)))
        .thenAnswer(invocation -> {
          LOG.debug("initiateMultipartUpload for {}", mockClient);
          synchronized (lock) {
            if (results.requests.size() == errors.failOnInit) {
              if (errors.recover) {
                errors.failOnInit(-1);
              }
              throw new AmazonClientException(
                  "Mock Fail on init " + results.requests.size());
            }
            String uploadId = UUID.randomUUID().toString();
            InitiateMultipartUploadRequest req = getArgumentAt(invocation,
                0, InitiateMultipartUploadRequest.class);
            results.requests.put(uploadId, req);
            results.activeUploads.put(uploadId, req.getKey());
            results.uploads.add(uploadId);
            return newResult(results.requests.get(uploadId), uploadId);
          }
        });

    // uploadPart
    when(mockClient.uploadPart(any(UploadPartRequest.class)))
        .thenAnswer(invocation -> {
          LOG.debug("uploadPart for {}", mockClient);
          synchronized (lock) {
            if (results.parts.size() == errors.failOnUpload) {
              if (errors.recover) {
                errors.failOnUpload(-1);
              }
              LOG.info("Triggering upload failure");
              throw new AmazonClientException(
                  "Mock Fail on upload " + results.parts.size());
            }
            UploadPartRequest req = getArgumentAt(invocation,
                0, UploadPartRequest.class);
            results.parts.add(req);
            String etag = UUID.randomUUID().toString();
            List<String> etags = results.tagsByUpload.get(req.getUploadId());
            if (etags == null) {
              etags = Lists.newArrayList();
              results.tagsByUpload.put(req.getUploadId(), etags);
            }
            etags.add(etag);
            return newResult(req, etag);
          }
        });

    // completeMultipartUpload
    when(mockClient
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenAnswer(invocation -> {
          LOG.debug("completeMultipartUpload for {}", mockClient);
          synchronized (lock) {
            if (results.commits.size() == errors.failOnCommit) {
              if (errors.recover) {
                errors.failOnCommit(-1);
              }
              throw new AmazonClientException(
                  "Mock Fail on commit " + results.commits.size());
            }
            CompleteMultipartUploadRequest req = getArgumentAt(invocation,
                0, CompleteMultipartUploadRequest.class);
            results.commits.add(req);
            results.activeUploads.remove(req.getUploadId());

            return newResult(req);
          }
        });

    // abortMultipartUpload mocking
    doAnswer(invocation -> {
      LOG.debug("abortMultipartUpload for {}", mockClient);
      synchronized (lock) {
        if (results.aborts.size() == errors.failOnAbort) {
          if (errors.recover) {
            errors.failOnAbort(-1);
          }
          throw new AmazonClientException(
              "Mock Fail on abort " + results.aborts.size());
        }
        AbortMultipartUploadRequest req = getArgumentAt(invocation,
            0, AbortMultipartUploadRequest.class);
        String id = req.getUploadId();
        String p = results.activeUploads.remove(id);
        if (p == null) {
          // upload doesn't exist
          AmazonS3Exception ex = new AmazonS3Exception(
              "not found " + id);
          ex.setStatusCode(404);
          throw ex;
        }
        results.aborts.add(req);
        return null;
      }
    })
        .when(mockClient)
        .abortMultipartUpload(any(AbortMultipartUploadRequest.class));

    // deleteObject mocking
    doAnswer(invocation -> {
      LOG.debug("deleteObject for {}", mockClient);
      synchronized (lock) {
        results.deletes.add(getArgumentAt(invocation,
            0, DeleteObjectRequest.class));
        return null;
      }
    })
        .when(mockClient)
        .deleteObject(any(DeleteObjectRequest.class));

    // deleteObject mocking
    doAnswer(invocation -> {
      LOG.debug("deleteObject for {}", mockClient);
      synchronized (lock) {
        results.deletes.add(new DeleteObjectRequest(
            getArgumentAt(invocation, 0, String.class),
            getArgumentAt(invocation, 1, String.class)
        ));
        return null;
      }
    }).when(mockClient)
        .deleteObject(any(String.class), any(String.class));

    // to String returns the debug information
    when(mockClient.toString()).thenAnswer(
        invocation -> "Mock3AClient " + results + " " + errors);

    when(mockClient
        .listMultipartUploads(any(ListMultipartUploadsRequest.class)))
        .thenAnswer(invocation -> {
          synchronized (lock) {
            MultipartUploadListing l = new MultipartUploadListing();
            l.setMultipartUploads(
                results.activeUploads.entrySet().stream()
                    .map(e -> newMPU(e.getKey(), e.getValue()))
                    .collect(Collectors.toList()));
            return l;
          }
        });

    return mockClient;
  }

  private static CompleteMultipartUploadResult newResult(
      CompleteMultipartUploadRequest req) {
    return new CompleteMultipartUploadResult();
  }


  private static MultipartUpload newMPU(String id, String path) {
    MultipartUpload up = new MultipartUpload();
    up.setUploadId(id);
    up.setKey(path);
    return up;
  }

  private static UploadPartResult newResult(UploadPartRequest request,
      String etag) {
    UploadPartResult result = new UploadPartResult();
    result.setPartNumber(request.getPartNumber());
    result.setETag(etag);
    return result;
  }

  private static InitiateMultipartUploadResult newResult(
      InitiateMultipartUploadRequest request, String uploadId) {
    InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
    result.setUploadId(uploadId);
    return result;
  }

  /**
   * create files in the attempt path that should be found by
   * {@code getTaskOutput}.
   * @param relativeFiles list of files relative to address path
   * @param attemptPath attempt path
   * @param conf config for FS
   * @throws IOException on any failure
   */
  public static void createTestOutputFiles(List<String> relativeFiles,
      Path attemptPath,
      Configuration conf) throws IOException {
    //
    FileSystem attemptFS = attemptPath.getFileSystem(conf);
    attemptFS.delete(attemptPath, true);
    for (String relative : relativeFiles) {
      // 0-length files are ignored, so write at least one byte
      OutputStream out = attemptFS.create(new Path(attemptPath, relative));
      out.write(34);
      out.close();
    }
  }

}
