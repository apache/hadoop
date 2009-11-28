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

package org.apache.hadoop.sqoop.io;

import java.io.OutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;
import java.util.Formatter;

import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An output stream that writes to an underlying filesystem, opening
 * a new file after a specified number of bytes have been written to the
 * current one.
 */
public class SplittingOutputStream extends OutputStream {

  public static final Log LOG = LogFactory.getLog(
      SplittingOutputStream.class.getName());

  private OutputStream writeStream;
  private CountingOutputStream countingFilterStream;
  private Configuration conf;
  private Path destDir;
  private String filePrefix;
  private long cutoffBytes;
  private boolean doGzip;
  private int fileNum;

  /**
   * Create a new SplittingOutputStream.
   * @param conf the Configuration to use to interface with HDFS
   * @param destDir the directory where the files will go (should already
   *     exist).
   * @param filePrefix the first part of the filename, which will be appended
   *    by a number. This file will be placed inside destDir.
   * @param cutoff the approximate number of bytes to use per file
   * @param doGzip if true, then output files will be gzipped and have a .gz
   *   suffix.
   */
  public SplittingOutputStream(final Configuration conf, final Path destDir,
      final String filePrefix, final long cutoff, final boolean doGzip)
      throws IOException {

    this.conf = conf;
    this.destDir = destDir;
    this.filePrefix = filePrefix;
    this.cutoffBytes = cutoff;
    if (this.cutoffBytes < 0) {
      this.cutoffBytes = 0; // splitting disabled.
    }
    this.doGzip = doGzip;
    this.fileNum = 0;

    openNextFile();
  }

  /** Initialize the OutputStream to the next file to write to.
   */
  private void openNextFile() throws IOException {
    FileSystem fs = FileSystem.get(conf);

    StringBuffer sb = new StringBuffer();
    Formatter fmt = new Formatter(sb);
    fmt.format("%05d", this.fileNum++);
    String filename = filePrefix + fmt.toString();
    if (this.doGzip) {
      filename = filename + ".gz";
    }
    Path destFile = new Path(destDir, filename);
    LOG.debug("Opening next output file: " + destFile);
    if (fs.exists(destFile)) {
      Path canonicalDest = destFile.makeQualified(fs);
      throw new IOException("Destination file " + canonicalDest
          + " already exists");
    }

    OutputStream fsOut = fs.create(destFile);

    // Count how many actual bytes hit HDFS.
    this.countingFilterStream = new CountingOutputStream(fsOut);

    if (this.doGzip) {
      // Wrap that in a Gzip stream.
      this.writeStream = new GZIPOutputStream(this.countingFilterStream);
    } else {
      // Write to the counting stream directly.
      this.writeStream = this.countingFilterStream;
    }
  }

  /**
   * @return true if allowSplit() would actually cause a split.
   */
  public boolean wouldSplit() {
    return this.cutoffBytes > 0
        && this.countingFilterStream.getByteCount() >= this.cutoffBytes;
  }

  /** If we've written more to the disk than the user's split size,
   * open the next file.
   */
  private void checkForNextFile() throws IOException {
    if (wouldSplit()) {
      LOG.debug("Starting new split");
      this.writeStream.flush();
      this.writeStream.close();
      openNextFile();
    }
  }

  /** Defines a point in the stream when it is acceptable to split to a new
      file; e.g., the end of a record.
    */
  public void allowSplit() throws IOException {
    checkForNextFile();
  }

  public void close() throws IOException {
    this.writeStream.close();
  }

  public void flush() throws IOException {
    this.writeStream.flush();
  }

  public void write(byte [] b) throws IOException {
    this.writeStream.write(b);
  }

  public void write(byte [] b, int off, int len) throws IOException {
    this.writeStream.write(b, off, len);
  }

  public void write(int b) throws IOException {
    this.writeStream.write(b);
  }
}
