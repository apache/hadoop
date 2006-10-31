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
import java.util.ArrayList;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A multiplexed OutputFormat. The channel choice is encoded within the key.
 * If channels are fed at the same rate then the data can be read back in 
 * with a TupleInputFormat. (in a different Job)
 * @see TupleInputFormat 
 * @author Michel Tourn
 */
public class MuxOutputFormat implements OutputFormat {

  public RecordWriter getRecordWriter(FileSystem fs, JobConf job, String name, Progressable progr) throws IOException {
    fs_ = fs;
    job_ = job;

    String primary = job.getOutputPath().toString();
    CompoundDirSpec spec = CompoundDirSpec.findOutputSpecForPrimary(primary, job);
    if (spec == null) {
      throw new IOException("Did not find -output spec in JobConf for primary:" + primary);
    }
    String[] outPaths = spec.getPaths()[0];
    int n = outPaths.length;
    RecordWriter[] writers = new RecordWriter[n];
    Path[] paths = new Path[n];
    for (int i = 0; i < n; i++) {
      OutputFormat f = new StreamOutputFormat(); // the only one supported
      writers[i] = f.getRecordWriter(fs, job, name, progr);
      paths[i] = new Path(outPaths[i], name); // same leaf name in different dir
    }
    return new MuxRecordWriter(writers, paths);
  }

  class MuxRecordWriter implements RecordWriter {

    MuxRecordWriter(RecordWriter[] writers, Path[] paths) throws IOException {
      writers_ = writers;
      paths_ = paths;
      numChannels_ = writers_.length;
      out_ = new FSDataOutputStream[numChannels_];
      for (int i = 0; i < out_.length; i++) {
        System.err.println("MuxRecordWriter [" + i + "] create: " + paths[i]);
        out_[i] = fs_.create(paths[i]);
      }
    }

    final static int ONE_BASED = 1;
    final static char CHANOUT = '>';
    final static char CHANIN = '<';
    final static String BADCHANOUT = "Invalid output channel spec: ";

    int parseOutputChannel(String s, int max) throws IOException {
      try {
        if (s.charAt(s.length() - 1) != CHANOUT) {
          throw new IOException(BADCHANOUT + s);
        }
        String s1 = s.substring(0, s.length() - 1);
        int c = Integer.parseInt(s1);
        if (c < 1 || c > max) {
          String msg = "Output channel '" + s + "': must be an integer between 1 and " + max
              + " followed by '" + CHANOUT + "' and TAB";
          throw new IndexOutOfBoundsException(msg);
        }
        return c;
      } catch (Exception e) {
        throw new IOException(BADCHANOUT + s + " cause:" + e);
      }
    }

    // TODO after Text patch, share code with StreamLineRecordReader.next()
    void splitFirstTab(String input, UTF8 first, UTF8 second) {
      int tab = input.indexOf('\t');
      if (tab == -1) {
        ((UTF8) first).set(input);
        ((UTF8) second).set("");
      } else {
        ((UTF8) first).set(input.substring(0, tab));
        ((UTF8) second).set(input);
      }

    }

    void writeKeyTabVal(Writable key, Writable val, FSDataOutputStream out) throws IOException {
      out.write(key.toString().getBytes("UTF-8"));
      out.writeByte('\t');
      out.write(val.toString().getBytes("UTF-8"));
      out.writeByte('\n');
    }

    public void write(WritableComparable key, Writable value) throws IOException {
      // convention: Application code must put a channel spec in first column
      // iff there is more than one (output) channel
      if (numChannels_ == 1) {
        writeKeyTabVal(key, value, out_[0]);
      } else {
        // StreamInputFormat does not know about channels 
        // Now reinterpret key as channel and split value as new key-value
        // A more general mechanism would still require Reader classes to know about channels. 
        // (and encode it as part of key or value)
        int channel = parseOutputChannel(key.toString(), numChannels_);
        FSDataOutputStream oi = out_[channel - ONE_BASED];
        splitFirstTab(value.toString(), key2, val2);
        writeKeyTabVal(key2, val2, oi);
      }
    }

    public void close(Reporter reporter) throws IOException {
      IOException firstErr = null;

      for (int i = 0; i < writers_.length; i++) {
        FSDataOutputStream oi = out_[i];
        RecordWriter r = writers_[i];
        try {
          oi.close();
          r.close(reporter);
        } catch (IOException io) {
          System.err.println("paths_[" + i + "]: " + paths_[i]);
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

    UTF8 key2 = new UTF8();
    UTF8 val2 = new UTF8();

    RecordWriter[] writers_;
    Path[] paths_;
    int numChannels_;
    FSDataOutputStream[] out_;
  }

  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    // allow existing data (for app-level restartability)
  }

  FileSystem fs_;
  JobConf job_;
}
