package org.apache.hadoop.yarn.logaggregation.testutils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class PathWithFileStatus {
  public final Path path;
  public FileStatus fileStatus;

  public PathWithFileStatus(Path path, FileStatus fileStatus) {
    this.path = path;
    this.fileStatus = fileStatus;
  }

  public void changeModificationTime(long modTime) {
    fileStatus = new FileStatus(fileStatus.getLen(), fileStatus.isDirectory(),
            fileStatus.getReplication(),
            fileStatus.getBlockSize(), modTime, fileStatus.getPath());
  }

  @Override
  public String toString() {
    return "PathWithFileStatus{" +
            "path=" + path +
            '}';
  }
}
