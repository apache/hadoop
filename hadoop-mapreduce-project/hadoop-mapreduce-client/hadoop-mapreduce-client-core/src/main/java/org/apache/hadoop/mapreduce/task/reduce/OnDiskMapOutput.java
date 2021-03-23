/**
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
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.mapred.IFileInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapOutputFile;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;
import org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl.CompressAwarePath;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class OnDiskMapOutput<K, V> extends IFileWrappedMapOutput<K, V> {
  private static final Logger LOG =
      LoggerFactory.getLogger(OnDiskMapOutput.class);
  private final FileSystem fs;
  private final Path tmpOutputPath;
  private final Path outputPath;
  private final OutputStream disk; 
  private long compressedSize;

  @Deprecated
  public OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput)
      throws IOException {
    this(mapId, merger, size, conf, fetcher,
        primaryMapOutput, FileSystem.getLocal(conf).getRaw(),
        mapOutputFile.getInputFileForWrite(mapId.getTaskID(), size));
  }

  @Deprecated
  OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K,V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput,
                         FileSystem fs, Path outputPath) throws IOException {
    this(mapId, merger, size, conf, fetcher, primaryMapOutput, fs, outputPath);
  }

  OnDiskMapOutput(TaskAttemptID mapId,
                  MergeManagerImpl<K, V> merger, long size,
                  JobConf conf,
                  int fetcher, boolean primaryMapOutput,
                  FileSystem fs, Path outputPath) throws IOException {
    super(conf, merger, mapId, size, primaryMapOutput);
    this.fs = fs;
    this.outputPath = outputPath;
    tmpOutputPath = getTempPath(outputPath, fetcher);
    disk = IntermediateEncryptedStream.wrapIfNecessary(conf,
        fs.create(tmpOutputPath), tmpOutputPath);
  }

  @VisibleForTesting
  static Path getTempPath(Path outPath, int fetcher) {
    return outPath.suffix(String.valueOf(fetcher));
  }

  @Override
  protected void doShuffle(MapHost host, IFileInputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    // Copy data to local-disk
    long bytesLeft = compressedLength;
    try {
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];
      while (bytesLeft > 0) {
        int n = input.readWithChecksum(buf, 0,
                                      (int) Math.min(bytesLeft, BYTES_TO_READ));
        if (n < 0) {
          throw new IOException("read past end of stream reading " + 
                                getMapId());
        }
        disk.write(buf, 0, n);
        bytesLeft -= n;
        metrics.inputBytes(n);
        reporter.progress();
      }

      LOG.info("Read " + (compressedLength - bytesLeft) + 
               " bytes from map-output for " + getMapId());

      disk.close();
    } catch (IOException ioe) {
      // Close the streams
      IOUtils.cleanupWithLogger(LOG, disk);

      // Re-throw
      throw ioe;
    }

    // Sanity check
    if (bytesLeft != 0) {
      throw new IOException("Incomplete map output received for " +
                            getMapId() + " from " +
                            host.getHostName() + " (" + 
                            bytesLeft + " bytes missing of " + 
                            compressedLength + ")");
    }
    this.compressedSize = compressedLength;
  }

  @Override
  public void commit() throws IOException {
    fs.rename(tmpOutputPath, outputPath);
    CompressAwarePath compressAwarePath = new CompressAwarePath(outputPath,
        getSize(), this.compressedSize);
    getMerger().closeOnDiskFile(compressAwarePath);
  }
  
  @Override
  public void abort() {
    try {
      fs.delete(tmpOutputPath, false);
    } catch (IOException ie) {
      LOG.info("failure to clean up " + tmpOutputPath, ie);
    }
  }

  @Override
  public String getDescription() {
    return "DISK";
  }

}
