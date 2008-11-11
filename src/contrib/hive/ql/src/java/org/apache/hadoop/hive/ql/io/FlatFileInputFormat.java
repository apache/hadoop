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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;
import java.io.DataInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;

import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Deserializer;

import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

/** An {@link InputFormat} for Plain files with {@link Deserializer} records */
public class FlatFileInputFormat<T> extends FileInputFormat<Void, FlatFileInputFormat.RowContainer<T>> {

  /**
   * A work-around until HADOOP-1230 is fixed. 
   *
   * Allows boolean next(k,v) to be called by reference but still allow the deserializer to create a new
   * object (i.e., row) on every call to next.
   */
  static public class RowContainer<T> {
    T row;
  }

  /**
   * An implementation of SerializationContext is responsible for looking up the Serialization implementation
   * for the given RecordReader. Potentially based on the Configuration or some other mechanism
   *
   * The SerializationFactory does not give this functionality since:
   *  1. Requires Serialization implementations to be specified in the Configuration a-priori (although same as setting
   *     a SerializationContext)
   *  2. Does not lookup the actual subclass being deserialized. e.g., for Serializable does not have a way of  configuring
   *      the actual Java class being serialized/deserialized.
   */
  static public interface SerializationContext<S> extends Configurable {

    /**
     *  An {@link Serialization} object for objects of type S
     * @return a serialization object for this context
     */
    public Serialization<S> getSerialization() throws IOException;

    /**
     *  Produces the specific class to deserialize
     */
    public Class<? extends S> getRealClass() throws IOException;
  }
  
  /**
   * The JobConf keys for the Serialization implementation
   */
  static public final String SerializationImplKey = "mapred.input.serialization.implKey";

  /**
   *  An implementation of {@link SerializationContext} that reads the Serialization class and 
   *  specific subclass to be deserialized from the JobConf.
   *
   */
  static public class SerializationContextFromConf<S> implements FlatFileInputFormat.SerializationContext<S> {

    /**
     * The JobConf keys for the Class that is being deserialized.
     */
    static public final String SerializationSubclassKey = "mapred.input.serialization.subclassKey";

    /**
     * Implements configurable so it can use the configuration to find the right classes
     * Note: ReflectionUtils will automatigically call setConf with the right configuration.
     */
    private Configuration conf;

    public void setConf(Configuration conf) { 
      this.conf = conf; 
    }

    public Configuration getConf() { 
      return conf; 
    }

    /**
     * @return the actual class being deserialized
     * @exception does not currently throw IOException
     */
    public Class<S> getRealClass() throws IOException {
      return (Class<S>)conf.getClass(SerializationSubclassKey, null, Object.class);
    }

    /**
     * Looks up and instantiates the Serialization Object
     *
     * Important to note here that we are not relying on the Hadoop SerializationFactory part of the 
     * Serialization framework. This is because in the case of Non-Writable Objects, we cannot make any
     * assumptions about the uniformity of the serialization class APIs - i.e., there may not be a "write"
     * method call and a subclass may need to implement its own Serialization classes. 
     * The SerializationFactory currently returns the first (de)serializer that is compatible
     * with the class to be deserialized;  in this context, that assumption isn't necessarily true.
     *
     * @return the serialization object for this context
     * @exception does not currently throw any IOException
     */
    public Serialization<S> getSerialization() throws IOException {
      Class<Serialization<S>> tClass = (Class<Serialization<S>>)conf.getClass(SerializationImplKey, null, Serialization.class);
      return tClass == null ? null : (Serialization<S>)ReflectionUtils.newInstance(tClass, conf);
    }
  }

  /** 
   * An {@link RecordReader} for plain files with {@link Deserializer} records 
   *
   * Reads one row at a time of type R.
   * R is intended to be a base class of something such as: Record, Writable, Text, ...
   *
   */
  public class FlatFileRecordReader<R> implements RecordReader<Void, FlatFileInputFormat.RowContainer<R>> {

    /**
     *  An interface for a helper class for instantiating {@link Serialization} classes.
     */
    /**
     * The stream in use - is fsin if not compressed, otherwise, it is dcin.
     */
    private final DataInputStream in;

    /**
     * The decompressed stream or null if the input is not decompressed.
     */
    private final InputStream dcin;

    /**
     * The underlying stream.
     */
    private final FSDataInputStream fsin;

    /**
     * For calculating progress
     */
    private final long end;

    /**
     * The constructed deserializer
     */
    private final Deserializer<R> deserializer;

    /**
     * Once EOF is reached, stop calling the deserializer 
     */
    private boolean isEOF;

    /**
     * The JobConf which contains information needed to instantiate the correct Deserializer
     */
    private Configuration conf;

    /**
     * The actual class of the row's we are deserializing, not just the base class
     */
    private Class<R> realRowClass;


    /**
     * FlatFileRecordReader constructor constructs the underlying stream (potentially decompressed) and 
     * creates the deserializer.
     *
     * @param conf the jobconf
     * @param split the split for this file
     */
    public FlatFileRecordReader(Configuration conf,
                                FileSplit split) throws IOException {
      final Path path = split.getPath();
      FileSystem fileSys = path.getFileSystem(conf);
      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
      final CompressionCodec codec = compressionCodecs.getCodec(path);
      this.conf = conf;

      fsin = fileSys.open(path);
      if (codec != null) {
        dcin = codec.createInputStream(fsin);
        in = new DataInputStream(dcin);
      } else {
        dcin = null;
        in = fsin;
      }

      isEOF = false;
      end = split.getLength();

      // Instantiate a SerializationContext which this will use to lookup the Serialization class and the 
      // actual class being deserialized
      SerializationContext<R> sinfo;
      Class<SerializationContext<R>> sinfoClass = 
        (Class<SerializationContext<R>>)conf.getClass(SerializationContextImplKey, SerializationContextFromConf.class);

      sinfo =  (SerializationContext<R>)ReflectionUtils.newInstance(sinfoClass, conf);

      // Get the Serialization object and the class being deserialized
      Serialization<R> serialization = sinfo.getSerialization();
      realRowClass  = (Class<R>)sinfo.getRealClass();

      deserializer = (Deserializer<R>)serialization.getDeserializer((Class<R>)realRowClass);
      deserializer.open(in);
    }

    /**
     * The actual class of the data being deserialized
     */
    private Class<R> realRowclass;

    /**
     * The JobConf key of the SerializationContext to use
     */
    static public final String SerializationContextImplKey = "mapred.input.serialization.context_impl";

    /**
     * @return null
     */
    public Void createKey() { 
      return null;
    }

    /**
     * @return a new R instance.
     */
    public RowContainer<R> createValue() { 
      RowContainer<R> r = new RowContainer<R>();
      r.row = (R)ReflectionUtils.newInstance(realRowClass, conf);
      return r;
    }

    /**
     * Returns the next row # and value
     *
     * @param key - void as these files have a value only
     * @param value - the row container which is always re-used, but the internal value may be set to a new Object
     * @return whether the key and value were read. True if they were and false if EOF
     * @exception IOException from the deserializer
     */
    public synchronized boolean next(Void key, RowContainer<R> value) throws IOException {
      if(isEOF  || in.available() == 0) {
        isEOF = true;
        return false;
      }

      // the deserializer is responsible for actually reading each record from the stream
      try {
        value.row = deserializer.deserialize(value.row);
        if (value.row == null) {
          isEOF = true;
          return false;
        }
        return true;
      } catch(EOFException e) {
        isEOF = true;
        return false;
      }
    }

    public synchronized float getProgress() throws IOException {
      // this assumes no splitting                                                                                               
      if (end == 0) {
        return 0.0f;
      } else {
        // gives progress over uncompressed stream                                                                               
        // assumes deserializer is not buffering itself
        return Math.min(1.0f, fsin.getPos()/(float)(end));
      }
    }

    public synchronized long getPos() throws IOException {
      // assumes deserializer is not buffering itself
      // position over uncompressed stream. not sure what                                                                        
      // effect this has on stats about job                                                                                      
      return fsin.getPos();
    }

    public synchronized void close() throws IOException {
      // assuming that this closes the underlying streams
      deserializer.close();
    }
  }

  protected boolean isSplittable(FileSystem fs, Path filename) {
    return false;
  }

  public RecordReader<Void, RowContainer<T>> getRecordReader(InputSplit split,
                                                             JobConf job, Reporter reporter)
    throws IOException {

    reporter.setStatus(split.toString());

    return new FlatFileRecordReader<T>(job, (FileSplit) split);
  }
}
