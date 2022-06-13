package org.apache.hadoop.yarn.logaggregation.testutils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;

public class FileStatusUtils {
  public static PathWithFileStatus createPathWithFileStatusForAppId(Path remoteRootLogDir,
                                                             ApplicationId appId,
                                                             String user, String suffix,
                                                             long modificationTime) {
    Path path = LogAggregationUtils.getRemoteAppLogDir(
            remoteRootLogDir, appId, user, suffix);
    FileStatus fileStatus = createEmptyFileStatus(modificationTime, path);
    return new PathWithFileStatus(path, fileStatus);
  }

  public static FileStatus createEmptyFileStatus(long modificationTime, Path path) {
    return new FileStatus(0, true, 0, 0, modificationTime, path);
  }

  public static PathWithFileStatus createFileLogPathWithFileStatus(Path baseDir, String childDir,
                                                            long modificationTime) {
    Path logPath = new Path(baseDir, childDir);
    FileStatus fStatus = createFileStatusWithLengthForFile(10, modificationTime, logPath);
    return new PathWithFileStatus(logPath, fStatus);
  }

  public static PathWithFileStatus createDirLogPathWithFileStatus(Path baseDir, String childDir,
                                                           long modificationTime) {
    Path logPath = new Path(baseDir, childDir);
    FileStatus fStatus = createFileStatusWithLengthForDir(10, modificationTime, logPath);
    return new PathWithFileStatus(logPath, fStatus);
  }

  public static PathWithFileStatus createDirBucketDirLogPathWithFileStatus(Path remoteRootLogPath,
                                                                    String user,
                                                                    String suffix,
                                                                    ApplicationId appId,
                                                                    long modificationTime) {
    Path bucketDir = LogAggregationUtils.getRemoteBucketDir(remoteRootLogPath, user, suffix, appId);
    FileStatus fStatus = new FileStatus(0, true, 0, 0, modificationTime, bucketDir);
    return new PathWithFileStatus(bucketDir, fStatus);
  }

  public static FileStatus createFileStatusWithLengthForFile(long length,
                                                              long modificationTime,
                                                              Path logPath) {
    return new FileStatus(length, false, 1, 1, modificationTime, logPath);
  }

  public static FileStatus createFileStatusWithLengthForDir(long length,
                                                             long modificationTime,
                                                             Path logPath) {
    return new FileStatus(length, true, 1, 1, modificationTime, logPath);
  }
}
