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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapOutputFile;

import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl.CompressAwarePath;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class OnDiskMapOutput<K, V> extends MapOutput<K, V> {
  private static final Log LOG = LogFactory.getLog(OnDiskMapOutput.class);
  private final FileSystem localFS;
  private final Path tmpOutputPath;
  private final Path outputPath;
  private final MergeManagerImpl<K, V> merger;
  private final OutputStream disk; 

  public OnDiskMapOutput(TaskAttemptID mapId, TaskAttemptID reduceId,
                         MergeManagerImpl<K, V> merger, long size,
                         JobConf conf,
                         MapOutputFile mapOutputFile,
                         int fetcher, boolean primaryMapOutput)
    throws IOException {
    super(mapId, size, primaryMapOutput);
    this.merger = merger;
    this.localFS = FileSystem.getLocal(conf);
    outputPath =
        mapOutputFile.getInputFileForWrite(mapId.getTaskID(),size);
    tmpOutputPath = outputPath.suffix(String.valueOf(fetcher));
    
    disk = localFS.create(tmpOutputPath);

  }

  @Override
  public void shuffle(MapHost host, InputStream input,
                      long compressedLength, long decompressedLength,
                      ShuffleClientMetrics metrics,
                      Reporter reporter) throws IOException {
    // Copy data to local-disk
    long bytesLeft = compressedLength;
    try {
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];
      while (bytesLeft > 0) {
        int n = input.read(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
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
      IOUtils.cleanup(LOG, input, disk);

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
  }

  @Override
  public void commit() throws IOException {
    localFS.rename(tmpOutputPath, outputPath);
    CompressAwarePath compressAwarePath = new CompressAwarePath(outputPath,
        getSize());
    merger.closeOnDiskFile(compressAwarePath);
  }
  
  @Override
  public void abort() {
    try {
      localFS.delete(tmpOutputPath, false);
    } catch (IOException ie) {
      LOG.info("failure to clean up " + tmpOutputPath, ie);
    }
  }

  @Override
  public String getDescription() {
    return "DISK";
  }
}
