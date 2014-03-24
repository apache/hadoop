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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf.SectionName;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.FileSummary;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.LimitInputStream;

/**
 * This is the tool for analyzing file sizes in the namespace image. In order to
 * run the tool one should define a range of integers <tt>[0, maxSize]</tt> by
 * specifying <tt>maxSize</tt> and a <tt>step</tt>. The range of integers is
 * divided into segments of size <tt>step</tt>:
 * <tt>[0, s<sub>1</sub>, ..., s<sub>n-1</sub>, maxSize]</tt>, and the visitor
 * calculates how many files in the system fall into each segment
 * <tt>[s<sub>i-1</sub>, s<sub>i</sub>)</tt>. Note that files larger than
 * <tt>maxSize</tt> always fall into the very last segment.
 *
 * <h3>Input.</h3>
 * <ul>
 * <li><tt>filename</tt> specifies the location of the image file;</li>
 * <li><tt>maxSize</tt> determines the range <tt>[0, maxSize]</tt> of files
 * sizes considered by the visitor;</li>
 * <li><tt>step</tt> the range is divided into segments of size step.</li>
 * </ul>
 *
 * <h3>Output.</h3> The output file is formatted as a tab separated two column
 * table: Size and NumFiles. Where Size represents the start of the segment, and
 * numFiles is the number of files form the image which size falls in this
 * segment.
 *
 */
final class FileDistributionCalculator {
  private final static long MAX_SIZE_DEFAULT = 0x2000000000L; // 1/8 TB = 2^37
  private final static int INTERVAL_DEFAULT = 0x200000; // 2 MB = 2^21
  private final static int MAX_INTERVALS = 0x8000000; // 128 M = 2^27

  private final Configuration conf;
  private final long maxSize;
  private final int steps;
  private final PrintWriter out;

  private final int[] distribution;
  private int totalFiles;
  private int totalDirectories;
  private int totalBlocks;
  private long totalSpace;
  private long maxFileSize;

  FileDistributionCalculator(Configuration conf, long maxSize, int steps,
      PrintWriter out) {
    this.conf = conf;
    this.maxSize = maxSize == 0 ? MAX_SIZE_DEFAULT : maxSize;
    this.steps = steps == 0 ? INTERVAL_DEFAULT : steps;
    this.out = out;
    long numIntervals = this.maxSize / this.steps;
    // avoid OutOfMemoryError when allocating an array
    Preconditions.checkState(numIntervals <= MAX_INTERVALS,
        "Too many distribution intervals (maxSize/step): " + numIntervals +
        ", should be less than " + (MAX_INTERVALS+1) + ".");
    this.distribution = new int[1 + (int) (numIntervals)];
  }

  void visit(RandomAccessFile file) throws IOException {
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FileSummary summary = FSImageUtil.loadSummary(file);
    FileInputStream in = null;
    try {
      in = new FileInputStream(file.getFD());
      for (FileSummary.Section s : summary.getSectionsList()) {
        if (SectionName.fromString(s.getName()) != SectionName.INODE) {
          continue;
        }

        in.getChannel().position(s.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                in, s.getLength())));
        run(is);
        output();
      }
    } finally {
      IOUtils.cleanup(null, in);
    }
  }

  private void run(InputStream in) throws IOException {
    INodeSection s = INodeSection.parseDelimitedFrom(in);
    for (int i = 0; i < s.getNumInodes(); ++i) {
      INodeSection.INode p = INodeSection.INode.parseDelimitedFrom(in);
      if (p.getType() == INodeSection.INode.Type.FILE) {
        ++totalFiles;
        INodeSection.INodeFile f = p.getFile();
        totalBlocks += f.getBlocksCount();
        long fileSize = 0;
        for (BlockProto b : f.getBlocksList()) {
          fileSize += b.getNumBytes();
        }
        maxFileSize = Math.max(fileSize, maxFileSize);
        totalSpace += fileSize * f.getReplication();

        int bucket = fileSize > maxSize ? distribution.length - 1 : (int) Math
            .ceil((double)fileSize / steps);
        ++distribution[bucket];

      } else if (p.getType() == INodeSection.INode.Type.DIRECTORY) {
        ++totalDirectories;
      }

      if (i % (1 << 20) == 0) {
        out.println("Processed " + i + " inodes.");
      }
    }
  }

  private void output() {
    // write the distribution into the output file
    out.print("Size\tNumFiles\n");
    for (int i = 0; i < distribution.length; i++) {
      if (distribution[i] != 0) {
        out.print(((long) i * steps) + "\t" + distribution[i]);
        out.print('\n');
      }
    }
    out.print("totalFiles = " + totalFiles + "\n");
    out.print("totalDirectories = " + totalDirectories + "\n");
    out.print("totalBlocks = " + totalBlocks + "\n");
    out.print("totalSpace = " + totalSpace + "\n");
    out.print("maxFileSize = " + maxFileSize + "\n");
  }
}
