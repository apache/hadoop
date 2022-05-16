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

package org.apache.hadoop.io;


import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** A file-based set of keys. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SetFile extends MapFile {

  protected SetFile() {}                            // no public ctor

  /** 
   * Write a new set file.
   */
  public static class Writer extends MapFile.Writer {

    /**
     * Create the named set for keys of the named class.
     * @deprecated pass a Configuration too
     * @param fs input FileSystem.
     * @param dirName input dirName.
     * @param keyClass input keyClass.
     * @throws IOException raised on errors performing I/O.
     */
    public Writer(FileSystem fs, String dirName,
	Class<? extends WritableComparable> keyClass) throws IOException {
      super(new Configuration(), fs, dirName, keyClass, NullWritable.class);
    }

    /**
     * Create a set naming the element class and compression type.
     *
     * @param conf input Configuration.
     * @param fs input FileSystem.
     * @param dirName input dirName.
     * @param keyClass input keyClass.
     * @param compress input compress.
     * @throws IOException raised on errors performing I/O.
     */
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  Class<? extends WritableComparable> keyClass,
                  SequenceFile.CompressionType compress)
      throws IOException {
      this(conf, fs, dirName, WritableComparator.get(keyClass, conf), compress);
    }

    /**
     * Create a set naming the element comparator and compression type.
     *
     * @param conf input Configuration.
     * @param fs input FileSystem.
     * @param dirName input dirName.
     * @param comparator input comparator.
     * @param compress input compress.
     * @throws IOException raised on errors performing I/O.
     */
    public Writer(Configuration conf, FileSystem fs, String dirName,
                  WritableComparator comparator,
                  SequenceFile.CompressionType compress) throws IOException {
      super(conf, new Path(dirName), 
            comparator(comparator), 
            valueClass(NullWritable.class), 
            compression(compress));
    }

    /**
     * Append a key to a set.  The key must be strictly greater than the
     * previous key added to the set.
     * @param key input key.
     * @throws IOException raised on errors performing I/O.
     */
    public void append(WritableComparable key) throws IOException{
      append(key, NullWritable.get());
    }
  }

  /** Provide access to an existing set file. */
  public static class Reader extends MapFile.Reader {

    /**
     * Construct a set reader for the named set.
     * @param fs input FileSystem.
     * @param dirName input dirName.
     * @param conf input Configuration.
     * @throws IOException raised on errors performing I/O.
     */
    public Reader(FileSystem fs, String dirName, Configuration conf) throws IOException {
      super(fs, dirName, conf);
    }

    /**
     * Construct a set reader for the named set using the named comparator.
     * @param fs input FileSystem.
     * @param dirName input dirName.
     * @param comparator input comparator.
     * @param conf input Configuration.
     * @throws IOException raised on errors performing I/O.
     */
    public Reader(FileSystem fs, String dirName, WritableComparator comparator, Configuration conf)
      throws IOException {
      super(new Path(dirName), conf, comparator(comparator));
    }

    // javadoc inherited
    @Override
    public boolean seek(WritableComparable key)
      throws IOException {
      return super.seek(key);
    }

    /**
     * Read the next key in a set into <code>key</code>.
     *
     * @param key input key.
     * @return Returns true if such a key exists
     *    and false when at the end of the set.
     * @throws IOException raised on errors performing I/O.
     */
    public boolean next(WritableComparable key)
      throws IOException {
      return next(key, NullWritable.get());
    }

    /**
     * Read the matching key from a set into <code>key</code>.
     *
     * @param key input key.
     * @return Returns <code>key</code>, or null if no match exists.
     * @throws IOException raised on errors performing I/O.
     */
    public WritableComparable get(WritableComparable key)
      throws IOException {
      if (seek(key)) {
        next(key);
        return key;
      } else
        return null;
    }
  }

}
