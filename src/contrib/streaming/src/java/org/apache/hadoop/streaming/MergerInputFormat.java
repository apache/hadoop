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

package org.apache.hadoop.streaming;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import org.apache.lucene.util.PriorityQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 Eventually will be fed TupleInputFormats. 
 For now will be fed primitive InputFormats.
 @author Michel Tourn
 */
public class MergerInputFormat extends InputFormatBase {

  public MergerInputFormat() {
  }

  void checkReady(FileSystem fs, JobConf job) {
    if (ready_) {
      // could complain if fs / job changes
      return;
    }
    fs_ = fs;
    job_ = job;
    debug_ = (job.get("stream.debug") != null);

    String someInputSpec = job_.get("stream.inputspecs.0");
    CompoundDirSpec someSpec = new CompoundDirSpec(someInputSpec, true);
    fmts_ = new ArrayList();
    int n = someSpec.paths_.length;
    inputTagged_ = job.getBoolean("stream.inputtagged", false);
    //  0 is primary
    //  Curr. secondaries are NOT used for getSplits(), only as RecordReader factory
    for (int i = 0; i < n; i++) {
      // this ignores -inputreader.. 
      // That's why if hasSimpleInputSpecs_=true (n=1) then StreamJob will set
      // the top-level format to StreamInputFormat rather than MergeInputFormat.
      // So we only support custom -inputformat for n=1. 
      // Probably OK for now since custom inputformats would be constrained (no \t and \n in payload) 
      fmts_.add(new StreamInputFormat()); // will be TupleInputFormat
    }
    primary_ = (InputFormat) fmts_.get(0);
    ready_ = true;
  }

  /** This implementation always returns true. */
  public boolean[] areValidInputDirectories(FileSystem fileSys, Path[] inputDirs) throws IOException {
    // must do this without JobConf...
    boolean[] b = new boolean[inputDirs.length];
    for (int i = 0; i < inputDirs.length; ++i) {
      b[i] = true;
    }
    return b;
  }

  /** Delegate to the primary InputFormat. 
   Force full-file splits since there's no index to sync secondaries.
   (and if there was, this index may need to be created for the first time
   full file at a time...    )
   */
  public FileSplit[] getSplits(FileSystem fs, JobConf job, int numSplits) throws IOException {
    checkReady(fs, job);
    return ((StreamInputFormat) primary_).getFullFileSplits(fs, job);
  }

  /**
   */
  public RecordReader getRecordReader(FileSystem fs, FileSplit split, JobConf job, Reporter reporter) throws IOException {
    checkReady(fs, job);

    reporter.setStatus(split.toString());

    ArrayList readers = new ArrayList();
    String primary = split.getPath().toString();
    CompoundDirSpec spec = CompoundDirSpec.findInputSpecForPrimary(primary, job);
    if (spec == null) {
      throw new IOException("Did not find -input spec in JobConf for primary:" + primary);
    }
    for (int i = 0; i < fmts_.size(); i++) {
      InputFormat f = (InputFormat) fmts_.get(i);
      Path path = new Path(spec.getPaths()[i][0]);
      FileSplit fsplit = makeFullFileSplit(path);
      RecordReader r = f.getRecordReader(fs, fsplit, job, reporter);
      readers.add(r);
    }

    return new MergedRecordReader(readers);
  }

  private FileSplit makeFullFileSplit(Path path) throws IOException {
    long len = fs_.getLength(path);
    return new FileSplit(path, 0, len);
  }

  /*
   private FileSplit relatedSplit(FileSplit primarySplit, int i, CompoundDirSpec spec) throws IOException
   {
   if(i == 0) {
   return primarySplit;
   }

   // TODO based on custom JobConf (or indirectly: InputFormat-s?)
   String path = primarySplit.getFile().getAbsolutePath();
   Path rpath = new Path(path + "." + i);

   long rlength = fs_.getLength(rpath);
   FileSplit related = new FileSplit(rpath, 0, rlength);
   return related;    
   }*/

  class MergedRecordReader implements RecordReader {

    MergedRecordReader(ArrayList/*<RecordReader>*/readers) throws IOException {
      try {
        readers_ = readers;
        primaryReader_ = (RecordReader) readers.get(0);
        q_ = new MergeQueue(readers.size(), debug_);
        for (int i = 0; i < readers_.size(); i++) {
          RecordReader reader = (RecordReader) readers.get(i);
          WritableComparable k = (WritableComparable) job_.getInputKeyClass().newInstance();
          Writable v = (Writable) job_.getInputValueClass().newInstance();
          MergeRecordStream si = new MergeRecordStream(i, reader, k, v);
          if (si.next()) {
            q_.add(si);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new IOException(e.toString());
      }
    }

    // 1. implements RecordReader

    public boolean next(Writable key, Writable value) throws IOException {
      boolean more = (q_.size() > 0);
      if (!more) return false;

      MergeRecordStream ms = (MergeRecordStream) q_.top();
      int keyTag = inputTagged_ ? (ms.index_ + 1) : NOTAG;
      assignTaggedWritable(key, ms.k_, keyTag);
      assignTaggedWritable(value, ms.v_, NOTAG);

      if (ms.next()) { // has another entry
        q_.adjustTop();
      } else {
        q_.pop(); // done with this file
        if (ms.reader_ == primaryReader_) {
          primaryClosed_ = true;
          primaryLastPos_ = primaryReader_.getPos();
        }
        ms.reader_.close();
      }
      return true;
    }

    public long getPos() throws IOException {
      if (primaryClosed_) {
        return primaryLastPos_;
      } else {
        return primaryReader_.getPos();
      }
    }

    public void close() throws IOException {
      IOException firstErr = null;

      for (int i = 0; i < readers_.size(); i++) {
        RecordReader r = (RecordReader) readers_.get(i);
        try {
          r.close();
        } catch (IOException io) {
          io.printStackTrace();
          if (firstErr == null) {
            firstErr = io;
          }
        }
      }
      if (firstErr != null) {
        throw firstErr;
      }
    }

    public WritableComparable createKey() {
      return new Text();
    }

    public Writable createValue() {
      return new Text();
    }

    // 2. utilities

    final static int NOTAG = -1;

    private void assignTaggedWritable(Writable dst, Writable src, int tag) {
      try {
        outBuf.reset();
        if (tag != NOTAG) {
          if (src instanceof UTF8) {
            src = new UTF8(">" + tag + "\t" + src.toString()); // breaks anything?
          } else if (src instanceof Text) {
            src = new Text(">" + tag + "\t" + src.toString()); // breaks anything?
          } else {
            throw new UnsupportedOperationException("Cannot use with tags with key class "
                + src.getClass());
          }
        }
        src.write(outBuf);
        inBuf.reset(outBuf.getData(), outBuf.getLength());
        dst.readFields(inBuf); // throws..
      } catch (IOException io) {
        // streams are backed by buffers, but buffers can run out
        throw new IllegalStateException(io);
      }
    }

    private DataInputBuffer inBuf = new DataInputBuffer();
    private DataOutputBuffer outBuf = new DataOutputBuffer();

    ArrayList/*<RecordReader>*/readers_;

    RecordReader primaryReader_;
    boolean primaryClosed_;
    long primaryLastPos_;

    MergeQueue q_;

  }

  boolean ready_;
  FileSystem fs_;
  JobConf job_;
  boolean debug_;

  // we need the JobConf: the other delegated InputFormat-s 
  // will only be created in the delegator RecordReader
  InputFormat primary_;
  boolean inputTagged_;
  ArrayList/*<InputFormat>*/fmts_;
}

class MergeQueue extends PriorityQueue // <MergeRecordStream>
{

  private boolean done;
  private boolean debug;

  public void add(MergeRecordStream reader) throws IOException {
    super.put(reader);
  }

  public MergeQueue(int size, boolean debug) throws IOException {
    initialize(size);
    this.debug = debug;
  }

  protected boolean lessThan(Object a, Object b) {
    MergeRecordStream ra = (MergeRecordStream) a;
    MergeRecordStream rb = (MergeRecordStream) b;
    int cp = ra.k_.compareTo(rb.k_);
    if (debug) {
      System.err.println("MergerInputFormat:lessThan " + ra.k_ + ", " + rb.k_ + " cp=" + cp);
    }
    if (cp == 0) {
      return (ra.index_ < rb.index_);
    } else {
      return (cp < 0);
    }
  }

  public void close() throws IOException {
    IOException firstErr = null;
    MergeRecordStream mr;
    while ((mr = (MergeRecordStream) pop()) != null) {
      try {
        mr.reader_.close();
      } catch (IOException io) {
        io.printStackTrace();
        if (firstErr == null) {
          firstErr = io;
        }
      }
    }
    if (firstErr != null) {
      throw firstErr;
    }
  }
}

class MergeRecordStream {

  int index_;
  RecordReader reader_;
  WritableComparable k_;
  Writable v_;

  public MergeRecordStream(int index, RecordReader reader, WritableComparable k, Writable v)
      throws IOException {
    index_ = index;
    reader_ = reader;
    k_ = k;
    v_ = v;
  }

  public boolean next() throws IOException {
    boolean more = reader_.next(k_, v_);
    return more;
  }
}
