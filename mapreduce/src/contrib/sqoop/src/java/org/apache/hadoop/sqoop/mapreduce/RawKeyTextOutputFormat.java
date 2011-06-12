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

package org.apache.hadoop.sqoop.mapreduce;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;

/** An {@link OutputFormat} that writes plain text files.
 * Only writes the key. Does not write any delimiter/newline after the key.
 */
public class RawKeyTextOutputFormat<K, V> extends FileOutputFormat<K, V> {

  protected static class RawKeyRecordWriter<K, V> extends RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";

    protected DataOutputStream out;

    public RawKeyRecordWriter(DataOutputStream out) {
      this.out = out;
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(utf8));
      }
    }

    public synchronized void write(K key, V value) throws IOException {
      writeObject(key);
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
      out.close();
    }
  }

  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
      throws IOException {
    boolean isCompressed = getCompressOutput(context);
    Configuration conf = context.getConfiguration();
    String ext = "";
    CompressionCodec codec = null;

    if (isCompressed) {
      // create the named codec
      Class<? extends CompressionCodec> codecClass =
        getOutputCompressorClass(context, GzipCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, conf);

      ext = codec.getDefaultExtension();
    }

    Path file = getDefaultWorkFile(context, ext);
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);
    DataOutputStream ostream = fileOut;

    if (isCompressed) {
      ostream = new DataOutputStream(codec.createOutputStream(fileOut));
    }

    return new RawKeyRecordWriter<K, V>(ostream);
  }
}

