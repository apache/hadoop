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

package org.apache.hadoop.mapred;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.LzopCodec;
import org.apache.hadoop.io.compress.LzopCodec.LzopDecompressor;
import org.apache.hadoop.util.LineReader;

/**
 * An {@link InputFormat} for lzop compressed text files. Files are broken into
 * lines. Either linefeed or carriage-return are used to signal end of line.
 * Keys are the position in the file, and values are the line of text.
 */
public class LzoTextInputFormat extends FileInputFormat<LongWritable, Text>
    implements JobConfigurable {

  private static final Log LOG
    = LogFactory.getLog(LzoTextInputFormat.class.getName());
  
  public static final String LZO_INDEX_SUFFIX = ".index";

  public void configure(JobConf conf) {
    FileInputFormat.setInputPathFilter(conf, LzopFilter.class);
  }

  /**
   * We don't want to process the index files.
   */
  static class LzopFilter implements PathFilter {
    public boolean accept(Path path) {
      if (path.toString().endsWith(LZO_INDEX_SUFFIX)) {
        return false;
      }
      return true;
    }
  }

  protected boolean isSplitable(FileSystem fs, Path file) {
    Path indexFile = new Path(file.toString()
        + LzoTextInputFormat.LZO_INDEX_SUFFIX);

    try {
      // can't split without the index
      return fs.exists(indexFile);
    } catch (IOException e) {
      LOG.warn("Could not check if index file exists", e);
      return false;
    }
  }

  public RecordReader<LongWritable, Text> getRecordReader(
      InputSplit genericSplit, JobConf job, Reporter reporter)
    throws IOException {

    reporter.setStatus(genericSplit.toString());
    return new LzoLineRecordReader(job, (FileSplit) genericSplit);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    FileSplit[] splits = (FileSplit[]) super.getSplits(job, numSplits);
    // find new start/ends of the filesplit that aligns
    // with the lzo blocks

    List<FileSplit> result = new ArrayList<FileSplit>();
    FileSystem fs = FileSystem.get(job);

    Map<Path, LzoIndex> indexes = new HashMap<Path, LzoIndex>();
    for (int i = 0; i < splits.length; i++) {
      FileSplit fileSplit = splits[i];
      // load the index
      Path file = fileSplit.getPath();
      if (!indexes.containsKey(file)) {
        LzoIndex index = readIndex(file, fs);
        if (index.isEmpty()) {
          // keep it as is since we didn't find an index
          result.add(fileSplit);
          continue;
        }

        indexes.put(file, index);
      }

      LzoIndex index = indexes.get(file);
      long start = fileSplit.getStart();
      long end = start + fileSplit.getLength();

      if (start != 0) {
        // find the next block position from
        // the start of the split
        long newStart = index.findNextPosition(start);
        if (newStart == -1 || newStart >= end) {
          // just skip this since it will be handled by another split
          continue;
        }
        start = newStart;
      }

      long newEnd = index.findNextPosition(end);
      if (newEnd != -1) {
        end = newEnd;
      }

      result.add(new FileSplit(file, start, end - start, fileSplit
          .getLocations()));
    }

    return result.toArray(new FileSplit[] {});
  }

  /**
   * Read the index of the lzo file.
   * 
   * @param split Read the index of this file.
   * @param fs The index file is on this file system.
   * @throws IOException
   */
  private LzoIndex readIndex(Path file, FileSystem fs) throws IOException {
    FSDataInputStream indexIn = null;
    try {
      Path indexFile = new Path(file.toString() + LZO_INDEX_SUFFIX);
      if (!fs.exists(indexFile)) {
        // return empty index, fall back to the unsplittable mode
        return new LzoIndex();
      }
      
      long indexLen = fs.getFileStatus(indexFile).getLen();
      int blocks = (int) (indexLen / 8);
      LzoIndex index = new LzoIndex(blocks);
      indexIn = fs.open(indexFile);
      for (int i = 0; i < blocks; i++) {
        index.set(i, indexIn.readLong());
      }
      return index;
    } finally {
      if (indexIn != null) {
        indexIn.close();
      }
    }
  }

  /**
   * Index an lzo file to allow the input format to split them into separate map
   * jobs.
   * 
   * @param fs File system that contains the file.
   * @param lzoFile the lzo file to index.
   * @throws IOException
   */
  public static void createIndex(FileSystem fs, Path lzoFile) 
    throws IOException {
    
    Configuration conf = fs.getConf();
    LzopCodec codec = new LzopCodec();
    codec.setConf(conf);

    FSDataInputStream is = null;
    FSDataOutputStream os = null;
    try {
      is = fs.open(lzoFile);
      os = fs.create(new Path(lzoFile.toString()
          + LzoTextInputFormat.LZO_INDEX_SUFFIX));
      LzopDecompressor decompressor = (LzopDecompressor) codec
          .createDecompressor();
      // for reading the header
      codec.createInputStream(is, decompressor);

      int numChecksums = decompressor.getChecksumsCount();

      while (true) {
        //read and ignore, we just want to get to the next int
        int uncompressedBlockSize = is.readInt();
        if (uncompressedBlockSize == 0) {
          break;
        } else if (uncompressedBlockSize < 0) {
          throw new EOFException();
        }
        
        int compressedBlockSize = is.readInt();
        if (compressedBlockSize <= 0) {
          throw new IOException("Could not read compressed block size");
        }

        long pos = is.getPos();
        // write the pos of the block start
        os.writeLong(pos - 8);
        // seek to the start of the next block, skip any checksums
        is.seek(pos + compressedBlockSize + (4 * numChecksums));
      }
    } finally {
      if (is != null) {
        is.close();
      }

      if (os != null) {
        os.close();
      }
    }
  }

  /**
   * Represents the lzo index.
   */
  static class LzoIndex {
    
    private long[] blockPositions;

    LzoIndex() {
    }   
    
    LzoIndex(int blocks) {
      blockPositions = new long[blocks];
    }
    
    /**
     * Set the position for the block.
     * @param blockNumber Block to set pos for.
     * @param pos Position.
     */
    public void set(int blockNumber, long pos) {
      blockPositions[blockNumber] = pos;
    }
    
    /**
     * Find the next lzo block start from the given position.
     * @param pos The position to start looking from.
     * @return Either the start position of the block or -1 if 
     * it couldn't be found.
     */
    public long findNextPosition(long pos) {
      int block = Arrays.binarySearch(blockPositions, pos);

      if(block >= 0) {
        //direct hit on a block start position
        return blockPositions[block];
      } else {
        block = Math.abs(block) - 1;
        if(block > blockPositions.length - 1) {
          return -1;
        }
        return blockPositions[block];
      }
    }

    public boolean isEmpty() {
      return blockPositions == null || blockPositions.length == 0;
    }    
    
  }
  
  /**
   * Reads line from an lzo compressed text file. Treats keys as offset in file
   * and value as line.
   */
  static class LzoLineRecordReader implements RecordReader<LongWritable, Text> {

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private FSDataInputStream fileIn;

    public LzoLineRecordReader(Configuration job, FileSplit split)
      throws IOException {

      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();

      FileSystem fs = file.getFileSystem(job);

      compressionCodecs = new CompressionCodecFactory(job);
      final CompressionCodec codec = compressionCodecs.getCodec(file);
      if (codec == null) {
        throw new IOException("No lzo codec found, cannot run");
      }

      // open the file and seek to the start of the split
      fileIn = fs.open(split.getPath());

      // creates input stream and also reads the file header
      in = new LineReader(codec.createInputStream(fileIn), job);

      if (start != 0) {
        fileIn.seek(start);

        // read and ignore the first line
        in.readLine(new Text());
        start = fileIn.getPos();
      }

      this.pos = start;
    }

    public LongWritable createKey() {
      return new LongWritable();
    }

    public Text createValue() {
      return new Text();
    }

    /** Read a line. */
    public synchronized boolean next(LongWritable key, Text value)
      throws IOException {

      //since the lzop codec reads everything in lzo blocks
      //we can't stop if the pos == end
      //instead we wait for the next block to be read in when
      //pos will be > end
      while (pos <= end) {
        key.set(pos);

        int newSize = in.readLine(value);
        if (newSize == 0) {
          return false;
        }
        pos = fileIn.getPos();

        return true;
      }

      return false;
    }

    /**
     * Get the progress within the split.
     */
    public float getProgress() {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (pos - start) / (float) (end - start));
      }
    }

    public synchronized long getPos() throws IOException {
      return pos;
    }

    public synchronized void close() throws IOException {
      if (in != null) {
        in.close();
      }
    }
  }

}
