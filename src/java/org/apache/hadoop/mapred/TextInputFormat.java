/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.UTF8;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class TextInputFormat extends InputFormatBase {

  public RecordReader getRecordReader(FileSystem fs, FileSplit split,
                                      JobConf job, Reporter reporter)
    throws IOException {

    reporter.setStatus(split.toString());

    final long start = split.getStart();
    final long end = start + split.getLength();

    // open the file and seek to the start of the split
    final FSDataInputStream in = fs.open(split.getFile());
    
    if (start != 0) {
      in.seek(start-1);
      while (in.getPos() < end) {    // scan to the next newline in the file
        char c = (char)in.read();
        if (c == '\r' || c == '\n') {
          break;
        }
      }
    }

    return new RecordReader() {
        /** Read a line. */
        public synchronized boolean next(Writable key, Writable value)
          throws IOException {
          long pos = in.getPos();
          if (pos >= end)
            return false;

          ((LongWritable)key).set(pos);           // key is position
          ((UTF8)value).set(readLine(in));        // value is line
          return true;
        }
        
        public  synchronized long getPos() throws IOException {
          return in.getPos();
        }

        public synchronized void close() throws IOException { in.close(); }

      };
  }

  private static String readLine(FSDataInputStream in) throws IOException {
    StringBuffer buffer = new StringBuffer();
    while (true) {

      int b = in.read();
      if (b == -1)
        break;

      char c = (char)b;              // bug: this assumes eight-bit characters.
      if (c == '\r' || c == '\n')
        break;

      buffer.append(c);
    }
    
    return buffer.toString();
  }

}

