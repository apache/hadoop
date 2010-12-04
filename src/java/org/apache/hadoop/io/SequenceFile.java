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

import java.io.*;
import java.util.*;
import java.rmi.server.UID;
import java.security.MessageDigest;
import org.apache.commons.logging.*;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.io.serial.RawComparator;
import org.apache.hadoop.io.serial.Serialization;
import org.apache.hadoop.io.serial.SerializationFactory;
import org.apache.hadoop.io.serial.TypedSerialization;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.MergeSort;
import org.apache.hadoop.util.PriorityQueue;

/** 
 * <code>SequenceFile</code>s are flat files consisting of binary key/value 
 * pairs.
 * 
 * <p><code>SequenceFile</code> provides {@link Writer}, {@link Reader} and
 * {@link Sorter} classes for writing, reading and sorting respectively.</p>
 * 
 * There are three <code>SequenceFile</code> <code>Writer</code>s based on the 
 * {@link CompressionType} used to compress key/value pairs:
 * <ol>
 *   <li>
 *   <code>Writer</code> : Uncompressed records.
 *   </li>
 *   <li>
 *   <code>RecordCompressWriter</code> : Record-compressed files, only compress 
 *                                       values.
 *   </li>
 *   <li>
 *   <code>BlockCompressWriter</code> : Block-compressed files, both keys & 
 *                                      values are collected in 'blocks' 
 *                                      separately and compressed. The size of 
 *                                      the 'block' is configurable.
 * </ol>
 * 
 * <p>The actual compression algorithm used to compress key and/or values can be
 * specified by using the appropriate {@link CompressionCodec}.</p>
 * 
 * <p>The recommended way is to use the static <tt>createWriter</tt> methods
 * provided by the <code>SequenceFile</code> to chose the preferred format.</p>
 *
 * <p>The {@link Reader} acts as the bridge and can read any of the above 
 * <code>SequenceFile</code> formats.</p>
 *
 * <h4 id="Formats">SequenceFile Formats</h4>
 * 
 * <p>Essentially there are 3 different formats for <code>SequenceFile</code>s
 * depending on the <code>CompressionType</code> specified. All of them share a
 * <a href="#Header">common header</a> described below.
 * 
 * <h5 id="Header">SequenceFile Header</h5>
 * <ul>
 *   <li>
 *   version - 3 bytes of magic header <b>SEQ</b>, followed by 1 byte of actual 
 *             version number (e.g. SEQ4 or SEQ6)
 *   </li>
 *   <li>
 *   key serialization name 
 *   </li>
 *   <li>
 *   key serialization configuration
 *   </li>
 *   <li>
 *   value serialization name
 *   </li>
 *   <li>
 *   value serialization data
 *   </li>
 *   <li>
 *   compression - A boolean which specifies if compression is turned on for 
 *                 keys/values in this file.
 *   </li>
 *   <li>
 *   blockCompression - A boolean which specifies if block-compression is 
 *                      turned on for keys/values in this file.
 *   </li>
 *   <li>
 *   compression codec - <code>CompressionCodec</code> class which is used for  
 *                       compression of keys and/or values (if compression is 
 *                       enabled).
 *   </li>
 *   <li>
 *   metadata - {@link Metadata} for this file.
 *   </li>
 *   <li>
 *   sync - A sync marker to denote end of the header.
 *   </li>
 * </ul>
 * 
 * <h5 id="#UncompressedFormat">Uncompressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record
 *   <ul>
 *     <li>Record length</li>
 *     <li>Key length</li>
 *     <li>Key</li>
 *     <li>Value</li>
 *   </ul>
 * </li>
 * <li>
 * A sync-marker every <code>2000</code> bytes or so.
 * </li>
 * </ul>
 *
 * <h5 id="#RecordCompressedFormat">Record-Compressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record
 *   <ul>
 *     <li>Record length</li>
 *     <li>Key length</li>
 *     <li>Key</li>
 *     <li><i>Compressed</i> Value</li>
 *   </ul>
 * </li>
 * <li>
 * A sync-marker every <code>2000</code> bytes or so.
 * </li>
 * </ul>
 * 
 * <h5 id="#BlockCompressedFormat">Block-Compressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record <i>Block</i>
 *   <ul>
 *     <li>sync-marker</li>
 *     <li>Compressed key-lengths block-size</li>
 *     <li>Compressed key-lengths block</li>
 *     <li>Compressed keys block-size</li>
 *     <li>Compressed keys block</li>
 *     <li>Compressed value-lengths block-size</li>
 *     <li>Compressed value-lengths block</li>
 *     <li>Compressed values block-size</li>
 *     <li>Compressed values block</li>
 *   </ul>
 * </li>
 * </ul>
 * 
 * <p>The compressed blocks of key lengths and value lengths consist of the 
 * actual lengths of individual keys/values encoded in ZeroCompressedInteger 
 * format.</p>
 * 
 * @see CompressionCodec
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SequenceFile {
  private static final Log LOG = LogFactory.getLog(SequenceFile.class);

  private SequenceFile() {}                         // no public ctor

  private static final byte BLOCK_COMPRESS_VERSION = (byte)4;
  private static final byte CUSTOM_COMPRESS_VERSION = (byte)5;
  private static final byte VERSION_WITH_METADATA = (byte)6;
  private static final byte SERIALIZATION_VERSION = (byte) 7;
  private static byte[] VERSION = new byte[] {
    (byte)'S', (byte)'E', (byte)'Q', SERIALIZATION_VERSION
  };

  private static final int SYNC_ESCAPE = -1;      // "length" of sync entries
  private static final int SYNC_HASH_SIZE = 16;   // number of bytes in hash 
  private static final int SYNC_SIZE = 4+SYNC_HASH_SIZE; // escape + hash

  /** The number of bytes between sync points.*/
  public static final int SYNC_INTERVAL = 100*SYNC_SIZE; 

  /** 
   * The compression type used to compress key/value pairs in the 
   * {@link SequenceFile}.
   * 
   * @see SequenceFile.Writer
   */
  public static enum CompressionType {
    /** Do not compress records. */
    NONE, 
    /** Compress values only, each separately. */
    RECORD,
    /** Compress sequences of records together in blocks. */
    BLOCK
  }

  /**
   * Get the compression type for the reduce outputs
   * @param job the job config to look in
   * @return the kind of compression to use
   */
  static public CompressionType getDefaultCompressionType(Configuration job) {
    String name = job.get("io.seqfile.compression.type");
    return name == null ? CompressionType.RECORD : 
      CompressionType.valueOf(name);
  }
  
  /**
   * Set the default compression type for sequence files.
   * @param job the configuration to modify
   * @param val the new compression type (none, block, record)
   */
  static public void setDefaultCompressionType(Configuration job, 
                                               CompressionType val) {
    job.set("io.seqfile.compression.type", val.toString());
  }

  /**
   * Create a new Writer with the given options.
   * @param conf the configuration to use
   * @param opts the options to create the file with
   * @return a new Writer
   * @throws IOException
   */
  public static Writer createWriter(Configuration conf, Writer.Option... opts
                                    ) throws IOException {
    Writer.CompressionOption compressionOption = 
      Options.getOption(Writer.CompressionOption.class, opts);
    CompressionType kind;
    if (compressionOption != null) {
      kind = compressionOption.getValue();
    } else {
      kind = getDefaultCompressionType(conf);
      opts = Options.prependOptions(opts, Writer.compression(kind));
    }
    switch (kind) {
      default:
      case NONE:
        return new Writer(conf, opts);
      case RECORD:
        return new RecordCompressWriter(conf, opts);
      case BLOCK:
        return new BlockCompressWriter(conf, opts);
    }
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static Writer 
    createWriter(FileSystem fs, Configuration conf, Path name, 
                 Class keyClass, Class valClass) throws IOException {
    return createWriter(conf, Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass));
  }
  
  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static Writer 
    createWriter(FileSystem fs, Configuration conf, Path name, 
                 Class keyClass, Class valClass, 
                 CompressionType compressionType) throws IOException {
    return createWriter(conf, Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.compression(compressionType));
  }
  
  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param progress The Progressable object to track progress.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static Writer
    createWriter(FileSystem fs, Configuration conf, Path name, 
                 Class keyClass, Class valClass, CompressionType compressionType,
                 Progressable progress) throws IOException {
    return createWriter(conf, Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.compression(compressionType),
                        Writer.progressable(progress));
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static Writer 
    createWriter(FileSystem fs, Configuration conf, Path name, 
                 Class keyClass, Class valClass, CompressionType compressionType, 
                 CompressionCodec codec) throws IOException {
    return createWriter(conf, Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.compression(compressionType, codec));
  }
  
  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param progress The Progressable object to track progress.
   * @param metadata The metadata of the file.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static Writer
    createWriter(FileSystem fs, Configuration conf, Path name, 
                 Class keyClass, Class valClass, 
                 CompressionType compressionType, CompressionCodec codec,
                 Progressable progress, Metadata metadata) throws IOException {
    return createWriter(conf, Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass),
                        Writer.compression(compressionType, codec),
                        Writer.progressable(progress),
                        Writer.metadata(metadata));
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem.
   * @param conf The configuration.
   * @param name The name of the file.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param bufferSize buffer size for the underlaying outputstream.
   * @param replication replication factor for the file.
   * @param blockSize block size for the file.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param progress The Progressable object to track progress.
   * @param metadata The metadata of the file.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static Writer
    createWriter(FileSystem fs, Configuration conf, Path name,
                 Class keyClass, Class valClass, int bufferSize,
                 short replication, long blockSize,
                 CompressionType compressionType, CompressionCodec codec,
                 Progressable progress, Metadata metadata) throws IOException {
    return createWriter(conf, Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.bufferSize(bufferSize), 
                        Writer.replication(replication),
                        Writer.blockSize(blockSize),
                        Writer.compression(compressionType, codec),
                        Writer.progressable(progress),
                        Writer.metadata(metadata));
  }

  /**
   * Construct the preferred type of SequenceFile Writer.
   * @param fs The configured filesystem. 
   * @param conf The configuration.
   * @param name The name of the file. 
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param progress The Progressable object to track progress.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public static Writer
    createWriter(FileSystem fs, Configuration conf, Path name, 
                 Class keyClass, Class valClass, 
                 CompressionType compressionType, CompressionCodec codec,
                 Progressable progress) throws IOException {
    return createWriter(conf, Writer.file(name), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass),
                        Writer.compression(compressionType, codec),
                        Writer.progressable(progress));
  }

  /**
   * Construct the preferred type of 'raw' SequenceFile Writer.
   * @param conf The configuration.
   * @param out The stream on top which the writer is to be constructed.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @param metadata The metadata of the file.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public static Writer
    createWriter(Configuration conf, FSDataOutputStream out, 
                 Class keyClass, Class valClass,
                 CompressionType compressionType,
                 CompressionCodec codec, Metadata metadata) throws IOException {
    return createWriter(conf, Writer.stream(out), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass), 
                        Writer.compression(compressionType, codec),
                        Writer.metadata(metadata));
  }
  
  /**
   * Construct the preferred type of 'raw' SequenceFile Writer.
   * @param conf The configuration.
   * @param out The stream on top which the writer is to be constructed.
   * @param keyClass The 'key' type.
   * @param valClass The 'value' type.
   * @param compressionType The compression type.
   * @param codec The compression codec.
   * @return Returns the handle to the constructed SequenceFile Writer.
   * @throws IOException
   * @deprecated Use {@link #createWriter(Configuration, Writer.Option...)}
   *     instead.
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public static Writer
    createWriter(Configuration conf, FSDataOutputStream out, 
                 Class keyClass, Class valClass, CompressionType compressionType,
                 CompressionCodec codec) throws IOException {
    return createWriter(conf, Writer.stream(out), Writer.keyClass(keyClass),
                        Writer.valueClass(valClass),
                        Writer.compression(compressionType, codec));
  }
  

  /** The interface to 'raw' values of SequenceFiles. */
  public static interface ValueBytes {

    /** Writes the uncompressed bytes to the outStream.
     * @param outStream : Stream to write uncompressed bytes into.
     * @throws IOException
     */
    public void writeUncompressedBytes(DataOutputStream outStream)
      throws IOException;

    /** Write compressed bytes to outStream. 
     * Note: that it will NOT compress the bytes if they are not compressed.
     * @param outStream : Stream to write compressed bytes into.
     */
    public void writeCompressedBytes(DataOutputStream outStream) 
      throws IllegalArgumentException, IOException;

    /**
     * Size of stored data.
     */
    public int getSize();
    
  }

  /**
   * Make an InputStream from a ValueBytes.
   * @param bytes the bytes to provide as input
   * @return a new input stream with the bytes
   * @throws IOException
   */
  private static InputStream readUncompressedBytes(ValueBytes bytes
                                                  ) throws IOException {
    DataInputBuffer result = new DataInputBuffer();
    if (bytes instanceof UncompressedBytes) {
      MutableValueBytes concrete = (MutableValueBytes) bytes;
      result.reset(concrete.data, concrete.dataSize);
    } else {
      DataOutputBuffer outBuf = new DataOutputBuffer();
      bytes.writeUncompressedBytes(outBuf);
      result.reset(outBuf.getData(), outBuf.getLength());
    }
    return result;
  }

  
  private static abstract class MutableValueBytes implements ValueBytes {
    protected byte[] data;
    protected int dataSize;

    MutableValueBytes() {
      data = null;
      dataSize = 0;
    }

    public int getSize() {
      return dataSize;
    }
    
    void reset(DataInputStream in, int length) throws IOException {
      if (data == null) {
        data = new byte[length];
      } else if (length > data.length) {
        data = new byte[Math.max(length, data.length * 2)];
      }
      dataSize = -1;
      in.readFully(data, 0, length);
      dataSize = length;
    }

    void set(MutableValueBytes other) {
      data = other.data;
      dataSize = other.dataSize;
    }
  }

  private static class UncompressedBytes extends MutableValueBytes {
    
    public void writeUncompressedBytes(DataOutputStream outStream)
      throws IOException {
      outStream.write(data, 0, dataSize);
    }

    public void writeCompressedBytes(DataOutputStream outStream) 
      throws IllegalArgumentException, IOException {
      throw 
        new IllegalArgumentException("UncompressedBytes cannot be compressed!");
    }

  } // UncompressedBytes
  
  private static class CompressedBytes extends MutableValueBytes {
    DataInputBuffer rawData = null;
    CompressionCodec codec = null;
    CompressionInputStream decompressedStream = null;

    private CompressedBytes(CompressionCodec codec) {
      this.codec = codec;
    }

    public void writeUncompressedBytes(DataOutputStream outStream)
      throws IOException {
      if (decompressedStream == null) {
        rawData = new DataInputBuffer();
        decompressedStream = codec.createInputStream(rawData);
      } else {
        decompressedStream.resetState();
      }
      rawData.reset(data, 0, dataSize);

      byte[] buffer = new byte[8192];
      int bytesRead = 0;
      while ((bytesRead = decompressedStream.read(buffer, 0, 8192)) != -1) {
        outStream.write(buffer, 0, bytesRead);
      }
    }

    public void writeCompressedBytes(DataOutputStream outStream) 
      throws IllegalArgumentException, IOException {
      outStream.write(data, 0, dataSize);
    }

  } // CompressedBytes
  
  /**
   * The class encapsulating with the metadata of a file.
   * The metadata of a file is a list of attribute name/value
   * pairs of Text type.
   *
   */
  public static class Metadata implements Writable {

    private TreeMap<Text, Text> theMetadata;
    
    public Metadata() {
      this(new TreeMap<Text, Text>());
    }
    
    public Metadata(TreeMap<Text, Text> arg) {
      if (arg == null) {
        this.theMetadata = new TreeMap<Text, Text>();
      } else {
        this.theMetadata = arg;
      }
    }
    
    public Text get(Text name) {
      return this.theMetadata.get(name);
    }
    
    public void set(Text name, Text value) {
      this.theMetadata.put(name, value);
    }
    
    public TreeMap<Text, Text> getMetadata() {
      return new TreeMap<Text, Text>(this.theMetadata);
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeInt(this.theMetadata.size());
      Iterator<Map.Entry<Text, Text>> iter =
        this.theMetadata.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<Text, Text> en = iter.next();
        en.getKey().write(out);
        en.getValue().write(out);
      }
    }

    public void readFields(DataInput in) throws IOException {
      int sz = in.readInt();
      if (sz < 0) throw new IOException("Invalid size: " + sz + " for file metadata object");
      this.theMetadata = new TreeMap<Text, Text>();
      for (int i = 0; i < sz; i++) {
        Text key = new Text();
        Text val = new Text();
        key.readFields(in);
        val.readFields(in);
        this.theMetadata.put(key, val);
      }    
    }

    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }
      if (other.getClass() != this.getClass()) {
        return false;
      } else {
        return equals((Metadata)other);
      }
    }
    
    public boolean equals(Metadata other) {
      if (other == null) return false;
      if (this.theMetadata.size() != other.theMetadata.size()) {
        return false;
      }
      Iterator<Map.Entry<Text, Text>> iter1 =
        this.theMetadata.entrySet().iterator();
      Iterator<Map.Entry<Text, Text>> iter2 =
        other.theMetadata.entrySet().iterator();
      while (iter1.hasNext() && iter2.hasNext()) {
        Map.Entry<Text, Text> en1 = iter1.next();
        Map.Entry<Text, Text> en2 = iter2.next();
        if (!en1.getKey().equals(en2.getKey())) {
          return false;
        }
        if (!en1.getValue().equals(en2.getValue())) {
          return false;
        }
      }
      if (iter1.hasNext() || iter2.hasNext()) {
        return false;
      }
      return true;
    }

    public int hashCode() {
      assert false : "hashCode not designed";
      return 42; // any arbitrary constant will do 
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("size: ").append(this.theMetadata.size()).append("\n");
      Iterator<Map.Entry<Text, Text>> iter =
        this.theMetadata.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<Text, Text> en = iter.next();
        sb.append("\t").append(en.getKey().toString()).append("\t").append(en.getValue().toString());
        sb.append("\n");
      }
      return sb.toString();
    }
  }
  
  /** Write key/value pairs to a sequence-format file. */
  public static class Writer implements java.io.Closeable {
    private Configuration conf;
    FSDataOutputStream out;
    boolean ownOutputStream = true;
    DataOutputBuffer buffer = new DataOutputBuffer();

    private final CompressionType compress;
    CompressionCodec codec = null;
    CompressionOutputStream deflateFilter = null;
    DataOutputStream deflateOut = null;
    Metadata metadata = null;
    Compressor compressor = null;
    
    protected Serialization<Object> keySerialization;
    protected Serialization<Object> valueSerialization;
    
    // Insert a globally unique 16-byte value every few entries, so that one
    // can seek into the middle of a file and then synchronize with record
    // starts and ends by scanning for this value.
    long lastSyncPos;                     // position of last sync
    byte[] sync;                          // 16 random bytes
    {
      try {                                       
        MessageDigest digester = MessageDigest.getInstance("MD5");
        long time = System.currentTimeMillis();
        digester.update((new UID()+"@"+time).getBytes());
        sync = digester.digest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public static interface Option {}
    
    static class FileOption extends Options.PathOption 
                                    implements Option {
      FileOption(Path path) {
        super(path);
      }
    }

    static class StreamOption extends Options.FSDataOutputStreamOption 
                              implements Option {
      StreamOption(FSDataOutputStream stream) {
        super(stream);
      }
    }

    static class BufferSizeOption extends Options.IntegerOption
                                  implements Option {
      BufferSizeOption(int value) {
        super(value);
      }
    }
    
    static class BlockSizeOption extends Options.LongOption implements Option {
      BlockSizeOption(long value) {
        super(value);
      }
    }

    static class ReplicationOption extends Options.IntegerOption
                                   implements Option {
      ReplicationOption(int value) {
        super(value);
      }
    }

    static class KeyClassOption extends Options.ClassOption implements Option {
      KeyClassOption(Class<?> value) {
        super(value);
      }
    }

    static class ValueClassOption extends Options.ClassOption
                                          implements Option {
      ValueClassOption(Class<?> value) {
        super(value);
      }
    }

    static class KeySerialization extends Options.SerializationOption 
                                  implements Option {
      KeySerialization(Serialization<?> value) {
        super(value);
      }
    }

    static class ValueSerialization extends Options.SerializationOption 
                                    implements Option {
      ValueSerialization(Serialization<?> value) {
        super(value);
      }
    }

    static class MetadataOption implements Option {
      private final Metadata value;
      MetadataOption(Metadata value) {
        this.value = value;
      }
      Metadata getValue() {
        return value;
      }
    }

    static class ProgressableOption extends Options.ProgressableOption
                                    implements Option {
      ProgressableOption(Progressable value) {
        super(value);
      }
    }

    private static class CompressionOption implements Option {
      private final CompressionType value;
      private final CompressionCodec codec;
      CompressionOption(CompressionType value) {
        this(value, null);
      }
      CompressionOption(CompressionType value, CompressionCodec codec) {
        this.value = value;
        this.codec = (CompressionType.NONE != value && null == codec)
          ? new DefaultCodec()
          : codec;
      }
      CompressionType getValue() {
        return value;
      }
      CompressionCodec getCodec() {
        return codec;
      }
    }
    
    public static Option file(Path value) {
      return new FileOption(value);
    }
    
    public static Option bufferSize(int value) {
      return new BufferSizeOption(value);
    }
    
    public static Option stream(FSDataOutputStream value) {
      return new StreamOption(value);
    }
    
    public static Option replication(short value) {
      return new ReplicationOption(value);
    }
    
    public static Option blockSize(long value) {
      return new BlockSizeOption(value);
    }
    
    public static Option progressable(Progressable value) {
      return new ProgressableOption(value);
    }

    public static Option keySerialization(Serialization<?> value) {
      return new KeySerialization(value);
    }
 
    public static Option valueSerialization(Serialization<?> value) {
      return new ValueSerialization(value);
    }
 
    public static Option keyClass(Class<?> value) {
      return new KeyClassOption(value);
    }
    
    public static Option valueClass(Class<?> value) {
      return new ValueClassOption(value);
    }
    
    public static Option metadata(Metadata value) {
      return new MetadataOption(value);
    }

    public static Option compression(CompressionType value) {
      return new CompressionOption(value);
    }

    public static Option compression(CompressionType value,
        CompressionCodec codec) {
      return new CompressionOption(value, codec);
    }
    
    /**
     * Construct a uncompressed writer from a set of options.
     * @param conf the configuration to use
     * @param options the options used when creating the writer
     * @throws IOException if it fails
     */
    @SuppressWarnings("unchecked")
    Writer(Configuration conf, 
           Option... opts) throws IOException {
      BlockSizeOption blockSizeOption = 
        Options.getOption(BlockSizeOption.class, opts);
      BufferSizeOption bufferSizeOption = 
        Options.getOption(BufferSizeOption.class, opts);
      ReplicationOption replicationOption = 
        Options.getOption(ReplicationOption.class, opts);
      ProgressableOption progressOption = 
        Options.getOption(ProgressableOption.class, opts);
      FileOption fileOption = Options.getOption(FileOption.class, opts);
      StreamOption streamOption = Options.getOption(StreamOption.class, opts);
      KeySerialization keySerializationOption = 
        Options.getOption(KeySerialization.class, opts);
      ValueSerialization valueSerializationOption = 
        Options.getOption(ValueSerialization.class, opts);
      KeyClassOption keyClassOption = 
        Options.getOption(KeyClassOption.class, opts);
      ValueClassOption valueClassOption = 
        Options.getOption(ValueClassOption.class, opts);
      MetadataOption metadataOption = 
        Options.getOption(MetadataOption.class, opts);
      CompressionOption compressionTypeOption =
        Options.getOption(CompressionOption.class, opts);
      // check consistency of options
      if ((fileOption == null) == (streamOption == null)) {
        throw new IllegalArgumentException("file or stream must be specified");
      }
      if (fileOption == null && (blockSizeOption != null ||
                                 bufferSizeOption != null ||
                                 replicationOption != null ||
                                 progressOption != null)) {
        throw new IllegalArgumentException("file modifier options not " +
                                           "compatible with stream");
      }
      // exactly one of serialization or class must be set.
      if ((keySerializationOption == null) == (keyClassOption == null)) {
        throw new IllegalArgumentException("Either keySerialization or " +
                                           " keyClass must be set.");
      }
      if ((valueSerializationOption == null) == (valueClassOption == null)) {
        throw new IllegalArgumentException("Either valueSerialization or " +
                                           " valueClass must be set.");
      }

      FSDataOutputStream out;
      boolean ownStream = fileOption != null;
      if (ownStream) {
        Path p = fileOption.getValue();
        FileSystem fs = p.getFileSystem(conf);
        int bufferSize = bufferSizeOption == null ? getBufferSize(conf) :
          bufferSizeOption.getValue();
        short replication = replicationOption == null ? 
          fs.getDefaultReplication() :
          (short) replicationOption.getValue();
        long blockSize = blockSizeOption == null ? fs.getDefaultBlockSize() :
          blockSizeOption.getValue();
        Progressable progress = progressOption == null ? null :
          progressOption.getValue();
        out = fs.create(p, true, bufferSize, replication, blockSize, progress);
      } else {
        out = streamOption.getValue();
      }
      
      // find the key serialization by parameter or by key type
      Serialization<Object> keySerialization;
      if (keyClassOption != null) {
        Class<?> keyClass = keyClassOption.getValue();
        SerializationFactory factory = SerializationFactory.getInstance(conf);
        keySerialization = 
          (Serialization<Object>) factory.getSerializationByType(keyClass);
      } else {
        keySerialization = 
          (Serialization<Object>) keySerializationOption.getValue();
      }

      // find the value serialization by parameter or by value type
      Serialization<Object> valueSerialization;
      if (valueClassOption != null) {
        Class<?> valueClass = valueClassOption.getValue();
        SerializationFactory factory = SerializationFactory.getInstance(conf);
        valueSerialization = 
          (Serialization<Object>) factory.getSerializationByType(valueClass);
      } else {
        valueSerialization = 
          (Serialization<Object>) valueSerializationOption.getValue();
      }

      Metadata metadata = metadataOption == null ?
          new Metadata() : metadataOption.getValue();
      this.compress = compressionTypeOption.getValue();
      final CompressionCodec codec = compressionTypeOption.getCodec();
      if (codec != null &&
          (codec instanceof GzipCodec) &&
          !NativeCodeLoader.isNativeCodeLoaded() &&
          !ZlibFactory.isNativeZlibLoaded(conf)) {
        throw new IllegalArgumentException("SequenceFile doesn't work with " +
                                           "GzipCodec without native-hadoop " +
                                           "code!");
      }
      init(conf, out, ownStream, keySerialization, valueSerialization, 
           codec, metadata);
    }

    /** Create the named file.
     * @deprecated Use 
     *   {@link SequenceFile#createWriter(Configuration, Writer.Option...)} 
     *   instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(FileSystem fs, Configuration conf, Path name, 
                  Class keyClass, Class valClass) throws IOException {
      this.compress = CompressionType.NONE;
      SerializationFactory factory = SerializationFactory.getInstance(conf);
      init(conf, fs.create(name), true, 
           factory.getSerializationByType(keyClass), 
           factory.getSerializationByType(valClass), null, 
           new Metadata());
    }
    
    /** Create the named file with write-progress reporter.
     * @deprecated Use 
     *   {@link SequenceFile#createWriter(Configuration, Writer.Option...)} 
     *   instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(FileSystem fs, Configuration conf, Path name, 
                  Class keyClass, Class valClass,
                  Progressable progress, Metadata metadata) throws IOException {
      this.compress = CompressionType.NONE;
      SerializationFactory factory = SerializationFactory.getInstance(conf);
      init(conf, fs.create(name, progress), true, 
           factory.getSerializationByType(keyClass), 
           factory.getSerializationByType(valClass),
           null, metadata);
    }
    
    /** Create the named file with write-progress reporter. 
     * @deprecated Use 
     *   {@link SequenceFile#createWriter(Configuration, Writer.Option...)} 
     *   instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Writer(FileSystem fs, Configuration conf, Path name,
                  Class keyClass, Class valClass,
                  int bufferSize, short replication, long blockSize,
                  Progressable progress, Metadata metadata) throws IOException {
      this.compress = CompressionType.NONE;
      SerializationFactory factory = SerializationFactory.getInstance(conf);
      init(conf,
           fs.create(name, true, bufferSize, replication, blockSize, progress),
           true, factory.getSerializationByType(keyClass), 
           factory.getSerializationByType(valClass), null, metadata);
    }

    boolean isCompressed() { return compress != CompressionType.NONE; }
    boolean isBlockCompressed() { return compress == CompressionType.BLOCK; }
    
    /** Write and flush the file header. */
    private void writeFileHeader() 
      throws IOException {
      out.write(VERSION);
      
      // write out key serialization
      Text.writeString(out, keySerialization.getName());
      buffer.reset();
      keySerialization.serializeSelf(buffer);
      WritableUtils.writeVInt(out, buffer.getLength());
      out.write(buffer.getData(), 0, buffer.getLength());
      
      // write out value serialization
      Text.writeString(out, valueSerialization.getName());
      buffer.reset();
      valueSerialization.serializeSelf(buffer);
      WritableUtils.writeVInt(out, buffer.getLength());
      out.write(buffer.getData(), 0, buffer.getLength());
      
      out.writeBoolean(this.isCompressed());
      out.writeBoolean(this.isBlockCompressed());
      
      if (this.isCompressed()) {
        Text.writeString(out, (codec.getClass()).getName());
      }
      this.metadata.write(out);
      out.write(sync);                       // write the sync bytes
      out.flush();                           // flush header
    }
    
    /** Initialize. */
    void init(Configuration conf, FSDataOutputStream out, boolean ownStream,
              Serialization<Object> keySerialization, 
              Serialization<Object> valueSerialization,
              CompressionCodec codec, Metadata metadata) 
      throws IOException {
      this.conf = conf;
      this.out = out;
      this.ownOutputStream = ownStream;
      this.keySerialization = keySerialization;
      this.valueSerialization = valueSerialization;
      this.codec = codec;
      this.metadata = metadata;
      if (this.codec != null) {
        ReflectionUtils.setConf(this.codec, this.conf);
        this.compressor = CodecPool.getCompressor(this.codec);
        this.deflateFilter = this.codec.createOutputStream(buffer, compressor);
        this.deflateOut = 
          new DataOutputStream(new BufferedOutputStream(deflateFilter));
      }
      writeFileHeader();
    }
    
    /** Returns the class of keys in this file. Only works for
     * if a TypedSerialization is used, otherwise Object is returned.
     * @deprecated Use {@link #getKeySerialization} instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Class getKeyClass() { 
      Class result = null;
      if (keySerialization instanceof TypedSerialization<?>) {
        TypedSerialization typed = (TypedSerialization) keySerialization;
        result = typed.getSpecificType();
      }
      return result == null ? Object.class : result;
    }

    
    /** Returns the class of values in this file. Only works for
     * if a TypedSerialization is used, otherwise Object is returned.  
     * @deprecated Use {@link #getValueSerialization} instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Class getValueClass() { 
      Class result = null;
      if (valueSerialization instanceof TypedSerialization<?>) {
        TypedSerialization typed = (TypedSerialization) valueSerialization;
        result = typed.getSpecificType();
      }
      return result == null ? Object.class : result;
    }

    /**
     * Return the serialization that is used to serialize the keys.
     * @return the key serialization
     */
    public Serialization<?> getKeySerialization() {
      return keySerialization;
    }
    
    /**
     * Return the serialization that is used to serialize the values.
     * @return the value serialization
     */
    public Serialization<?> getValueSerialization() {
      return valueSerialization;
    }

    /** Returns the compression codec of data in this file. */
    public CompressionCodec getCompressionCodec() { return codec; }
    
    /** create a sync point */
    public void sync() throws IOException {
      if (sync != null && lastSyncPos != out.getPos()) {
        out.writeInt(SYNC_ESCAPE);                // mark the start of the sync
        out.write(sync);                          // write sync
        lastSyncPos = out.getPos();               // update lastSyncPos
      }
    }

    /** Returns the configuration of this file. */
    Configuration getConf() { return conf; }
    
    /** Close the file. */
    public synchronized void close() throws IOException {
      CodecPool.returnCompressor(compressor);
      compressor = null;
      
      if (out != null) {
        
        // Close the underlying stream iff we own it...
        if (ownOutputStream) {
          out.close();
        } else {
          out.flush();
        }
        out = null;
      }
    }

    synchronized void checkAndWriteSync() throws IOException {
      if (sync != null &&
          out.getPos() >= lastSyncPos+SYNC_INTERVAL) { // time to emit sync
        sync();
      }
    }

    /** Append a key/value pair. 
     */
    public void append(Writable key, Writable val) throws IOException {
      append((Object) key, (Object) val);
    }

    /** Append a key/value pair. */
    public synchronized void append(Object key, Object val)
      throws IOException {

      buffer.reset();

      // Append the 'key'
      keySerialization.serialize(buffer, key);
      int keyLength = buffer.getLength();
      if (keyLength < 0)
        throw new IOException("negative length keys not allowed: " + key);

      // Append the 'value'
      if (compress == CompressionType.RECORD) {
        deflateFilter.resetState();
        valueSerialization.serialize(deflateFilter, val);
        deflateOut.flush();
        deflateFilter.finish();
      } else {
        valueSerialization.serialize(buffer, val);
      }

      // Write the record out
      checkAndWriteSync();                                // sync
      out.writeInt(buffer.getLength());                   // total record length
      out.writeInt(keyLength);                            // key portion length
      out.write(buffer.getData(), 0, buffer.getLength()); // data
    }

    public synchronized void appendRaw(byte[] keyData, int keyOffset,
        int keyLength, ValueBytes val) throws IOException {
      if (keyLength < 0)
        throw new IOException("negative length keys not allowed: " + keyLength);

      int valLength = val.getSize();

      checkAndWriteSync();
      
      out.writeInt(keyLength+valLength);          // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(keyData, keyOffset, keyLength);   // key
      val.writeUncompressedBytes(out);            // value
    }

    /** Returns the current length of the output file.
     *
     * <p>This always returns a synchronized position.  In other words,
     * immediately after calling {@link SequenceFile.Reader#seek(long)} with a position
     * returned by this method, {@link SequenceFile.Reader#next(Writable)} may be called.  However
     * the key may be earlier in the file than key last written when this
     * method was called (e.g., with block-compression, it may be the first key
     * in the block that was being written when this method was called).
     */
    public synchronized long getLength() throws IOException {
      return out.getPos();
    }

  } // class Writer

  /** Write key/compressed-value pairs to a sequence-format file. */
  static class RecordCompressWriter extends Writer {
    
    RecordCompressWriter(Configuration conf, 
                         Option... options) throws IOException {
      super(conf, options);
    }

    /** Append a key/value pair. */
    public synchronized void append(Object key, Object val) throws IOException {

      // Append the 'key'
      buffer.reset();
      keySerialization.serialize(buffer, key);
      int keyLength = buffer.getLength();
      if (keyLength < 0)
        throw new IOException("negative length keys not allowed: " + key);

      // Compress 'value' and append it
      deflateFilter.resetState();
      valueSerialization.serialize(deflateFilter, val);
      deflateOut.flush();
      deflateFilter.finish();

      // Write the record out
      checkAndWriteSync();                                // sync
      out.writeInt(buffer.getLength());                   // total record length
      out.writeInt(keyLength);                            // key portion length
      out.write(buffer.getData(), 0, buffer.getLength()); // data
    }

    /** Append a key/value pair. */
    public synchronized void appendRaw(byte[] keyData, int keyOffset,
        int keyLength, ValueBytes val) throws IOException {

      if (keyLength < 0)
        throw new IOException("negative length keys not allowed: " + keyLength);

      int valLength = val.getSize();
      
      checkAndWriteSync();                        // sync
      out.writeInt(keyLength+valLength);          // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(keyData, keyOffset, keyLength);   // 'key' data
      val.writeCompressedBytes(out);              // 'value' data
    }
    
  } // RecordCompressionWriter

  /** Write compressed key/value blocks to a sequence-format file. */
  static class BlockCompressWriter extends Writer {
    
    private int noBufferedRecords = 0;
    
    private DataOutputBuffer keyLenBuffer = new DataOutputBuffer();
    private DataOutputBuffer keyBuffer = new DataOutputBuffer();

    private DataOutputBuffer valLenBuffer = new DataOutputBuffer();
    private DataOutputBuffer valBuffer = new DataOutputBuffer();

    private final int compressionBlockSize;
    
    BlockCompressWriter(Configuration conf,
                        Option... options) throws IOException {
      super(conf, options);
      compressionBlockSize = 
        conf.getInt("io.seqfile.compress.blocksize", 1000000);
    }

    /** Workhorse to check and write out compressed data/lengths */
    private synchronized 
      void writeBuffer(DataOutputBuffer uncompressedDataBuffer) 
      throws IOException {
      deflateFilter.resetState();
      buffer.reset();
      deflateOut.write(uncompressedDataBuffer.getData(), 0, 
                       uncompressedDataBuffer.getLength());
      deflateOut.flush();
      deflateFilter.finish();
      
      WritableUtils.writeVInt(out, buffer.getLength());
      out.write(buffer.getData(), 0, buffer.getLength());
    }
    
    /** Compress and flush contents to dfs */
    public synchronized void sync() throws IOException {
      if (noBufferedRecords > 0) {
        super.sync();
        
        // No. of records
        WritableUtils.writeVInt(out, noBufferedRecords);
        
        // Write 'keys' and lengths
        writeBuffer(keyLenBuffer);
        writeBuffer(keyBuffer);
        
        // Write 'values' and lengths
        writeBuffer(valLenBuffer);
        writeBuffer(valBuffer);
        
        // Flush the file-stream
        out.flush();
        
        // Reset internal states
        keyLenBuffer.reset();
        keyBuffer.reset();
        valLenBuffer.reset();
        valBuffer.reset();
        noBufferedRecords = 0;
      }
      
    }
    
    /** Close the file. */
    public synchronized void close() throws IOException {
      if (out != null) {
        sync();
      }
      super.close();
    }

    /** Append a key/value pair. */
    public synchronized void append(Object key, Object val) throws IOException {

      // Save key/value into respective buffers 
      int oldKeyLength = keyBuffer.getLength();
      keySerialization.serialize(keyBuffer, key);
      int keyLength = keyBuffer.getLength() - oldKeyLength;
      if (keyLength < 0)
        throw new IOException("negative length keys not allowed: " + key);
      WritableUtils.writeVInt(keyLenBuffer, keyLength);

      int oldValLength = valBuffer.getLength();
      valueSerialization.serialize(valBuffer, val);
      int valLength = valBuffer.getLength() - oldValLength;
      WritableUtils.writeVInt(valLenBuffer, valLength);
      
      // Added another key/value pair
      ++noBufferedRecords;
      
      // Compress and flush?
      int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength();
      if (currentBlockSize >= compressionBlockSize) {
        sync();
      }
    }
    
    /** Append a key/value pair. */
    public synchronized void appendRaw(byte[] keyData, int keyOffset,
        int keyLength, ValueBytes val) throws IOException {
      
      if (keyLength < 0)
        throw new IOException("negative length keys not allowed");

      int valLength = val.getSize();
      
      // Save key/value data in relevant buffers
      WritableUtils.writeVInt(keyLenBuffer, keyLength);
      keyBuffer.write(keyData, keyOffset, keyLength);
      WritableUtils.writeVInt(valLenBuffer, valLength);
      val.writeUncompressedBytes(valBuffer);

      // Added another key/value pair
      ++noBufferedRecords;

      // Compress and flush?
      int currentBlockSize = keyBuffer.getLength() + valBuffer.getLength(); 
      if (currentBlockSize >= compressionBlockSize) {
        sync();
      }
    }
  
  } // BlockCompressionWriter

  /** Get the configured buffer size */
  private static int getBufferSize(Configuration conf) {
    return conf.getInt("io.file.buffer.size", 4096);
  }

  /** Reads key/value pairs from a sequence-format file. */
  public static class Reader implements java.io.Closeable {
    private String filename;
    private FSDataInputStream in;

    private byte version;

    private CompressionCodec codec = null;
    private Metadata metadata = null;
    
    private byte[] sync = new byte[SYNC_HASH_SIZE];
    private byte[] syncCheck = new byte[SYNC_HASH_SIZE];
    private boolean syncSeen;

    private long headerEnd;
    private long end;

    private boolean decompress;
    private boolean blockCompressed;
    
    private Configuration conf;

    private int noBufferedRecords = 0;
    
    private DataInputBuffer keyLenBuffer = null;
    private CompressionInputStream keyLenInFilter = null;
    private DataInputStream keyLenIn = null;
    private Decompressor keyLenDecompressor = null;
    private DataInputBuffer keyBlockBuffer = null;
    private CompressionInputStream keyInFilter = null;
    private DataInputStream keyIn = null;
    private Decompressor keyDecompressor = null;

    private DataInputBuffer valLenBuffer = null;
    private CompressionInputStream valLenInFilter = null;
    private DataInputStream valLenIn = null;
    private Decompressor valLenDecompressor = null;
    private DataInputBuffer valBuffer = null;
    private CompressionInputStream valInFilter = null;
    private DataInputStream valIn = null;
    private Decompressor valDecompressor = null;
    
    // used for object serialization
    private DataOutputBuffer keyBuffer;
    private MutableValueBytes valueBytes;
    private DataInputBuffer serialBuffer;

    private Serialization<Object> keySerialization;
    private Serialization<Object> valueSerialization;

    /**
     * A tag interface for all of the Reader options
     */
    public static interface Option {}
    
    /**
     * Create an option to specify the path name of the sequence file.
     * @param value the path to read
     * @return a new option
     */
    public static Option file(Path value) {
      return new FileOption(value);
    }
    
    /**
     * Create an option to specify the stream with the sequence file.
     * @param value the stream to read.
     * @return a new option
     */
    public static Option stream(FSDataInputStream value) {
      return new InputStreamOption(value);
    }
    
    /**
     * Create an option to specify the required key serialization.
     * @param value the serialization to deserialize the key with
     * @return a new option
     */
    public static Option keySerialization(Serialization<?> value) {
      return new KeySerializationOption(value);
    }
    
    /**
     * Create an option to specify the required value serialization.
     * @param value the serialization to deserialize the value with
     * @return a new option
     */
    public static Option valueSerialization(Serialization<?> value) {
      return new ValueSerializationOption(value);
    }

    /**
     * Create an option to specify the starting byte to read.
     * @param value the number of bytes to skip over
     * @return a new option
     */
    public static Option start(long value) {
      return new StartOption(value);
    }
    
    /**
     * Create an option to specify the number of bytes to read.
     * @param value the number of bytes to read
     * @return a new option
     */
    public static Option length(long value) {
      return new LengthOption(value);
    }
    
    /**
     * Create an option with the buffer size for reading the given pathname.
     * @param value the number of bytes to buffer
     * @return a new option
     */
    public static Option bufferSize(int value) {
      return new BufferSizeOption(value);
    }

    private static class FileOption extends Options.PathOption 
                                    implements Option {
      private FileOption(Path value) {
        super(value);
      }
    }
    
    private static class InputStreamOption
        extends Options.FSDataInputStreamOption 
        implements Option {
      private InputStreamOption(FSDataInputStream value) {
        super(value);
      }
    }

    private static class StartOption extends Options.LongOption
                                     implements Option {
      private StartOption(long value) {
        super(value);
      }
    }

    private static class LengthOption extends Options.LongOption
                                      implements Option {
      private LengthOption(long value) {
        super(value);
      }
    }

    private static class BufferSizeOption extends Options.IntegerOption
                                      implements Option {
      private BufferSizeOption(int value) {
        super(value);
      }
    }

    // only used directly
    private static class OnlyHeaderOption extends Options.BooleanOption 
                                          implements Option {
      private OnlyHeaderOption() {
        super(true);
      }
    }

    private static class KeySerializationOption 
                         extends Options.SerializationOption
                         implements Option {
      private KeySerializationOption(Serialization<?> value) {
        super(value);
      }
    }

    private static class ValueSerializationOption 
                         extends Options.SerializationOption
                         implements Option {
      private ValueSerializationOption(Serialization<?> value) {
        super(value);
      }
    }

    public Reader(Configuration conf, Option... opts) throws IOException {
      // Look up the options, these are null if not set
      FileOption fileOpt = Options.getOption(FileOption.class, opts);
      InputStreamOption streamOpt = 
        Options.getOption(InputStreamOption.class, opts);
      StartOption startOpt = Options.getOption(StartOption.class, opts);
      LengthOption lenOpt = Options.getOption(LengthOption.class, opts);
      BufferSizeOption bufOpt = Options.getOption(BufferSizeOption.class,opts);
      OnlyHeaderOption headerOnly = 
        Options.getOption(OnlyHeaderOption.class, opts);
      KeySerializationOption keyOpt = 
        Options.getOption(KeySerializationOption.class, opts);
      ValueSerializationOption valueOpt = 
        Options.getOption(ValueSerializationOption.class, opts);

      // check for consistency
      if ((fileOpt == null) == (streamOpt == null)) {
        throw new 
          IllegalArgumentException("File or stream option must be specified");
      }
      if (fileOpt == null && bufOpt != null) {
        throw new IllegalArgumentException("buffer size can only be set when" +
                                           " a file is specified.");
      }

      // figure out the real values
      Path filename = null;
      FSDataInputStream file;
      final long len;
      if (fileOpt != null) {
        filename = fileOpt.getValue();
        FileSystem fs = filename.getFileSystem(conf);
        int bufSize = bufOpt == null ? getBufferSize(conf): bufOpt.getValue();
        len = null == lenOpt
          ? fs.getFileStatus(filename).getLen()
          : lenOpt.getValue();
        file = openFile(fs, filename, bufSize, len);
      } else {
        len = null == lenOpt ? Long.MAX_VALUE : lenOpt.getValue();
        file = streamOpt.getValue();
      }
      long start = startOpt == null ? 0 : startOpt.getValue();

      // really set up
      initialize(filename, file, start, len, conf, 
                 (keyOpt == null ? null : keyOpt.getValue()),
                 (valueOpt == null ? null : valueOpt.getValue()),
                 headerOnly != null);
    }

    /**
     * Construct a reader by opening a file from the given file system.
     * @param fs The file system used to open the file.
     * @param file The file being read.
     * @param conf Configuration
     * @throws IOException
     * @deprecated Use Reader(Configuration, Option...) instead.
     */
    @Deprecated
    public Reader(FileSystem fs, Path file, 
                  Configuration conf) throws IOException {
      this(conf, file(file.makeQualified(fs)));
    }

    /**
     * Construct a reader by the given input stream.
     * @param in An input stream.
     * @param buffersize unused
     * @param start The starting position.
     * @param length The length being read.
     * @param conf Configuration
     * @throws IOException
     * @deprecated Use Reader(Configuration, Reader.Option...) instead.
     */
    @Deprecated
    public Reader(FSDataInputStream in, int buffersize,
        long start, long length, Configuration conf) throws IOException {
      this(conf, stream(in), start(start), length(length));
    }

    /** Common work of the constructors. */
    private void initialize(Path filename, FSDataInputStream in,
                            long start, long length, Configuration conf,
                            Serialization<?> keySerialization,
                            Serialization<?> valueSerialization,
                            boolean tempReader) throws IOException {
      if (in == null) {
        throw new IllegalArgumentException("in == null");
      }
      this.filename = filename == null ? "<unknown>" : filename.toString();
      this.in = in;
      this.conf = conf;
      boolean succeeded = false;
      try {
        seek(start);
        this.end = this.in.getPos() + length;
        // if it wrapped around, use the max
        if (end < length) {
          end = Long.MAX_VALUE;
        }
        init(tempReader, keySerialization, valueSerialization);
        succeeded = true;
      } finally {
        if (!succeeded) {
          IOUtils.cleanup(LOG, this.in);
        }
      }
    }

    /**
     * Override this method to specialize the type of
     * {@link FSDataInputStream} returned.
     * @param fs The file system used to open the file.
     * @param file The file being read.
     * @param bufferSize The buffer size used to read the file.
     * @param length The length being read if it is >= 0.  Otherwise,
     *               the length is not available.
     * @return The opened stream.
     * @throws IOException
     */
    protected FSDataInputStream openFile(FileSystem fs, Path file,
        int bufferSize, long length) throws IOException {
      return fs.open(file, bufferSize);
    }

    @SuppressWarnings("unchecked")
    private 
    Serialization<Object> readSerialization(SerializationFactory factory,
                                            Serialization<?> override
                                            ) throws IOException {
      String serializationName = Text.readString(in);
      Serialization<?> result;
      if (override == null) {
        result = factory.getSerialization(serializationName);
      } else {
        if (!serializationName.equals(override.getName())) {
          throw new IllegalArgumentException("using serialization " +
                                             override.getName() +
                                             " instead of " + 
                                             serializationName);
        }
        result = override;
      }
      int keySerialLength = WritableUtils.readVInt(in);
      DataInputBuffer buffer = new DataInputBuffer();
      byte[] bytes = new byte[keySerialLength];
      in.readFully(bytes);
      buffer.reset(bytes, keySerialLength);
      result.deserializeSelf(buffer, conf);
      return (Serialization<Object>) result;
    }

    /**
     * Initialize the {@link Reader}
     * @param tmpReader <code>true</code> if we are constructing a temporary
     *                  reader {@link SequenceFile.Sorter.cloneFileAttributes}, 
     *                  and hence do not initialize every component; 
     *                  <code>false</code> otherwise.
     * @throws IOException
     */
    @SuppressWarnings({ "unchecked", "deprecation" })
    private void init(boolean tempReader,
                      Serialization keySerialization,
                      Serialization valueSerialization) throws IOException {
      byte[] versionBlock = new byte[VERSION.length];
      in.readFully(versionBlock);

      if ((versionBlock[0] != VERSION[0]) ||
          (versionBlock[1] != VERSION[1]) ||
          (versionBlock[2] != VERSION[2]))
        throw new IOException(this + " not a SequenceFile");

      // Set 'version'
      version = versionBlock[3];
      if (version > VERSION[3])
        throw new VersionMismatchException(VERSION[3], version);

      SerializationFactory factory = SerializationFactory.getInstance(conf);
      if (version < SERIALIZATION_VERSION) {
        String keyClassName;
        String valueClassName;
        if (version < BLOCK_COMPRESS_VERSION) {
          UTF8 className = new UTF8();

          className.readFields(in);
          keyClassName = className.toString(); // key class name

          className.readFields(in);
          valueClassName = className.toString(); // val class name
        } else {
          keyClassName = Text.readString(in);
          valueClassName = Text.readString(in);
        }
        try {
          this.keySerialization = (Serialization<Object>)
            factory.getSerializationByType(conf.getClassByName(keyClassName));
        } catch (ClassNotFoundException cnf) {
          throw new RuntimeException("key class " + keyClassName +
                                     " not found");
        }
        try {
          this.valueSerialization = (Serialization<Object>)
            factory.getSerializationByType(conf.getClassByName(valueClassName));
        } catch (ClassNotFoundException cnf) {
          throw new RuntimeException("value class " + valueClassName +
                                     " not found");
        }
      } else {
        this.keySerialization = readSerialization(factory, keySerialization);
        this.valueSerialization = readSerialization(factory,valueSerialization);
      }

      if (version > 2) {                          // if version > 2
        this.decompress = in.readBoolean();       // is compressed?
      } else {
        decompress = false;
      }

      if (version >= BLOCK_COMPRESS_VERSION) {    // if version >= 4
        this.blockCompressed = in.readBoolean();  // is block-compressed?
      } else {
        blockCompressed = false;
      }
      
      // if version >= 5
      // setup the compression codec
      if (decompress) {
        if (version >= CUSTOM_COMPRESS_VERSION) {
          String codecClassname = Text.readString(in);
          try {
            Class<? extends CompressionCodec> codecClass
              = conf.getClassByName(codecClassname).asSubclass(CompressionCodec.class);
            this.codec = ReflectionUtils.newInstance(codecClass, conf);
          } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("Unknown codec: " + 
                                               codecClassname, cnfe);
          }
        } else {
          codec = new DefaultCodec();
          ((Configurable)codec).setConf(conf);
        }
      }
      
      this.metadata = new Metadata();
      if (version >= VERSION_WITH_METADATA) {    // if version >= 6
        this.metadata.readFields(in);
      }
      
      if (version > 1) {                          // if version > 1
        in.readFully(sync);                       // read sync bytes
        headerEnd = in.getPos();                  // record end of header
      }
      
      // Initialize... *not* if this we are constructing a temporary Reader
      if (!tempReader) {
        keyBuffer = new DataOutputBuffer();
        serialBuffer = new DataInputBuffer();
        valBuffer = new DataInputBuffer();
        if (decompress) {
          valDecompressor = CodecPool.getDecompressor(codec);
          valInFilter = codec.createInputStream(valBuffer, valDecompressor);
          valIn = new DataInputStream(valInFilter);
        } else {
          valIn = valBuffer;
        }

        if (blockCompressed) {
          keyLenBuffer = new DataInputBuffer();
          keyBlockBuffer = new DataInputBuffer();
          valLenBuffer = new DataInputBuffer();

          keyLenDecompressor = CodecPool.getDecompressor(codec);
          keyLenInFilter = codec.createInputStream(keyLenBuffer, 
                                                   keyLenDecompressor);
          keyLenIn = new DataInputStream(keyLenInFilter);

          keyDecompressor = CodecPool.getDecompressor(codec);
          keyInFilter = codec.createInputStream(keyBlockBuffer, 
                                                keyDecompressor);
          keyIn = new DataInputStream(keyInFilter);

          valLenDecompressor = CodecPool.getDecompressor(codec);
          valLenInFilter = codec.createInputStream(valLenBuffer, 
                                                   valLenDecompressor);
          valLenIn = new DataInputStream(valLenInFilter);
        }
        valueBytes = (MutableValueBytes) createValueBytes();
      }
    }
    
    /** Close the file. */
    public synchronized void close() throws IOException {
      // Return the decompressors to the pool
      CodecPool.returnDecompressor(keyLenDecompressor);
      CodecPool.returnDecompressor(keyDecompressor);
      CodecPool.returnDecompressor(valLenDecompressor);
      CodecPool.returnDecompressor(valDecompressor);
      keyLenDecompressor = keyDecompressor = null;
      valLenDecompressor = valDecompressor = null;
      
      // Close the input-stream
      in.close();
    }

    /**
     * Return the name of the key class. It only works for
     * TypedSerializations and otherwise returns Object.
     * @return the key class name
     * @deprecated Use {@link #getKeySerialization()} instead.
     */
    @Deprecated
    public String getKeyClassName() {
      return getKeyClass().getName();
    }

    /**
     * Get the class of the keys in this file. It only works for
     * TypedSerializations and otherwise returns Object.
     * @return the class of the keys
     * @deprecated Use {@link #getKeySerialization()} instead.
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public synchronized Class<?> getKeyClass() {
      Class result = null;
      if (keySerialization instanceof TypedSerialization) {
        TypedSerialization typed = (TypedSerialization) keySerialization;
        result = typed.getSpecificType();
      }
      return result == null ? Object.class : result;
    }

    /**
     * Return the name of the value class. It only works for
     * TypedSerializations and otherwise returns Object.
     * @return the value class name
     * @deprecated Use {@link #getValueSerialization()} instead.
     */
    @Deprecated
    public String getValueClassName() {
      return getValueClass().getName();
    }

    /**
     * Get the class of the values in this file. It only works for
     * TypedSerializations and otherwise returns Object.
     * @return the class of the values
     * @deprecated Use {@link #getValueSerialization()} instead.
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public synchronized Class<?> getValueClass() {
      Class result = null;
      if (valueSerialization instanceof TypedSerialization) {
        TypedSerialization typed = (TypedSerialization) valueSerialization;
        result = typed.getSpecificType();
      }
      return result == null ? Object.class : result;
    }

    /**
     * Get the serialization for the key.
     * @return the key serialization
     */
    public Serialization<?> getKeySerialization() {
      return keySerialization;
    }

    /**
     * Get the serialization for the value.
     * @return the value serialization
     */
    public Serialization<?> getValueSerialization() {
      return valueSerialization;
    }

    /** Returns true if values are compressed. */
    public boolean isCompressed() { return decompress; }
    
    /** Returns true if records are block-compressed. */
    public boolean isBlockCompressed() { return blockCompressed; }
    
    /** Returns the compression codec of data in this file. */
    public CompressionCodec getCompressionCodec() { return codec; }
    
    /**
     * Get the compression type for this file.
     * @return the compression type
     */
    public CompressionType getCompressionType() {
      if (decompress) {
        return blockCompressed ? CompressionType.BLOCK : CompressionType.RECORD;
      } else {
        return CompressionType.NONE;
      }
    }

    /** Returns the metadata object of the file */
    public Metadata getMetadata() {
      return this.metadata;
    }
    
    /** Returns the configuration used for this file. */
    Configuration getConf() { return conf; }
    
    /** Read a compressed buffer */
    private synchronized void readBuffer(DataInputBuffer buffer, 
                                         CompressionInputStream filter) throws IOException {
      // Read data into a temporary buffer
      DataOutputBuffer dataBuffer = new DataOutputBuffer();

      try {
        int dataBufferLength = WritableUtils.readVInt(in);
        dataBuffer.write(in, dataBufferLength);
      
        // Set up 'buffer' connected to the input-stream
        buffer.reset(dataBuffer.getData(), 0, dataBuffer.getLength());
      } finally {
        dataBuffer.close();
      }

      // Reset the codec
      filter.resetState();
    }
    
    /** Read the next 'compressed' block */
    private synchronized void readBlock() throws IOException {
      
      // Reset internal states
      noBufferedRecords = 0;

      //Process sync
      if (sync != null) {
        in.readInt();
        in.readFully(syncCheck);                // read syncCheck
        if (!Arrays.equals(sync, syncCheck))    // check it
          throw new IOException("File is corrupt!");
      }
      syncSeen = true;

      // Read number of records in this block
      noBufferedRecords = WritableUtils.readVInt(in);
      
      // Read key lengths and keys
      readBuffer(keyLenBuffer, keyLenInFilter);
      readBuffer(keyBlockBuffer, keyInFilter);
      
      // Read value lengths and values
      readBuffer(valLenBuffer, valLenInFilter);
      readBuffer(valBuffer, valInFilter);
    }

    /**
     * Get the 'value' corresponding to the last read 'key'.
     * @param val : The 'value' to be read.
     * @throws IOException
     */
    public synchronized Object getCurrentValue(Object val) throws IOException {
      return valueSerialization.deserialize(readUncompressedBytes(valueBytes), 
                                            val, conf);
    }

    /** Read the next key/value pair in the file into <code>key</code> and
     * <code>val</code>.  Returns true if such a pair exists and false when at
     * end of file
     * @deprecated Use {@link #next(Object)} and 
     *     {@link #getCurrentValue(Object)} to iterate through keys and values.
     */
    @Deprecated
    public synchronized boolean next(Writable key,
                                     Writable val) throws IOException {

      if (nextKey(key) == null) {
        return false;
      } else {
        getCurrentValue(val);
        return true;
      }
    }
    
    /**
     * Read and return the next record length, potentially skipping over 
     * a sync block.
     * @return the length of the next record or -1 if there is no next record
     * @throws IOException
     */
    private synchronized int readRecordLength() throws IOException {
      if (in.getPos() >= end) {
        return -1;
      }      
      int length = in.readInt();
      if (version > 1 && sync != null &&
          length == SYNC_ESCAPE) {              // process a sync entry
        in.readFully(syncCheck);                // read syncCheck
        if (!Arrays.equals(sync, syncCheck))    // check it
          throw new IOException("File is corrupt!");
        syncSeen = true;
        if (in.getPos() >= end) {
          return -1;
        }
        length = in.readInt();                  // re-read length
      } else {
        syncSeen = false;
      }
      
      return length;
    }
    
    public ValueBytes createValueBytes() {
      ValueBytes val = null;
      if (!decompress || blockCompressed) {
        val = new UncompressedBytes();
      } else {
        val = new CompressedBytes(codec);
      }
      return val;
    }

    /**
     * Read 'raw' records. Doesn't reset the key buffer. The new key appends
     * on to the current contents.
     * @param key - The buffer into which the key is read
     * @param value - The 'raw' value
     * @return Returns the total record length or -1 for end of file
     * @throws IOException
     */
    public synchronized int nextRaw(DataOutputBuffer key, 
                                    ValueBytes value) throws IOException {
      if (!blockCompressed) {
        int length = readRecordLength();
        if (length == -1) {
          return -1;
        }
        int keyLength = in.readInt();
        int valLength = length - keyLength;
        key.write(in, keyLength);
        ((MutableValueBytes) value).reset(in, valLength);
        
        return length;
      } else {
        //Reset syncSeen
        syncSeen = false;
        
        // Read 'key'
        if (noBufferedRecords == 0) {
          if (in.getPos() >= end) 
            return -1;

          readBlock();
        }
        int keyLength = WritableUtils.readVInt(keyLenIn);
        if (keyLength < 0) {
          throw new IOException("zero length key found!");
        }
        key.write(keyIn, keyLength);
        --noBufferedRecords;
        
        // Read raw 'value'
        int valLength = WritableUtils.readVInt(valLenIn);
        UncompressedBytes rawValue = (UncompressedBytes)value;
        rawValue.reset(valIn, valLength);
        
        return (keyLength+valLength);
      }
      
    }

    /**
     * Read 'raw' keys.
     * @param key - The buffer into which the key is read
     * @return Returns the key length or -1 for end of file
     * @throws IOException
     */
    public synchronized int nextRawKey(DataOutputBuffer key) throws IOException{
      key.reset();
      return nextRaw(key, valueBytes);
    }

    /**
     * Read the next key in the file.
     * The value is available via {@link #getCurrentValue}.
     * @param key if not null, may be used to hold the next key
     * @return true if a key was read, false if eof
     * @throws IOException
     * @deprecated Use {@link #nextKey} instead.
     */
    @Deprecated
    public boolean next(Writable key) throws IOException {
      return nextKey(key) != null;
    }

    /**
     * Read the next key from the file.
     * @param key if not null, may be used to hold the next key
     * @return the key that was read
     * @throws IOException
     * @deprecated Use {@link #nextKey} instead.
     */
    @Deprecated
    public Object next(Object key) throws IOException {
      return nextKey(key);
    }

    /** Read the next key in the file. 
     * The value is available via {@link #getCurrentValue}.
     */
    public synchronized Object nextKey(Object key) throws IOException {
      keyBuffer.reset();
      int recordLen = nextRaw(keyBuffer, valueBytes);
      if (recordLen < 0) {
        return null;
      }
      serialBuffer.reset(keyBuffer.getData(), keyBuffer.getLength());
      return keySerialization.deserialize(serialBuffer, key, conf);
    }

    /**
     * Read 'raw' values.
     * @param val - The 'raw' value
     * @return Returns the value length
     * @throws IOException
     */
    public synchronized int nextRawValue(ValueBytes val) throws IOException {
      ((MutableValueBytes) val).set(valueBytes);
      return val.getSize();
    }

    private void handleChecksumException(ChecksumException e)
      throws IOException {
      if (this.conf.getBoolean("io.skip.checksum.errors", false)) {
        LOG.warn("Bad checksum at "+getPosition()+". Skipping entries.");
        sync(getPosition()+this.conf.getInt("io.bytes.per.checksum", 512));
      } else {
        throw e;
      }
    }

    /** disables sync. often invoked for tmp files */
    synchronized void ignoreSync() {
      sync = null;
    }
    
    /** Set the current byte position in the input file.
     *
     * <p>The position passed must be a position returned by {@link
     * SequenceFile.Writer#getLength()} when writing this file.  To seek to an arbitrary
     * position, use {@link SequenceFile.Reader#sync(long)}.
     */
    public synchronized void seek(long position) throws IOException {
      in.seek(position);
      if (blockCompressed) {                      // trigger block read
        noBufferedRecords = 0;
      }
    }

    /** Seek to the next sync mark past a given position.*/
    public synchronized void sync(long position) throws IOException {
      if (position+SYNC_SIZE >= end) {
        seek(end);
        return;
      }

      if (position < headerEnd) {
        // seek directly to first record
        in.seek(headerEnd);
        // note the sync marker "seen" in the header
        syncSeen = true;
        return;
      }

      try {
        seek(position+4);                         // skip escape
        in.readFully(syncCheck);
        int syncLen = sync.length;
        for (int i = 0; in.getPos() < end; i++) {
          int j = 0;
          for (; j < syncLen; j++) {
            if (sync[j] != syncCheck[(i+j)%syncLen])
              break;
          }
          if (j == syncLen) {
            in.seek(in.getPos() - SYNC_SIZE);     // position before sync
            return;
          }
          syncCheck[i%syncLen] = in.readByte();
        }
      } catch (ChecksumException e) {             // checksum failure
        handleChecksumException(e);
      }
    }

    /** Returns true iff the previous call to next passed a sync mark.*/
    public synchronized boolean syncSeen() { return syncSeen; }

    /** Return the current byte position in the input file. */
    public synchronized long getPosition() throws IOException {
      return in.getPos();
    }

    /** Returns the name of the file. */
    public String toString() {
      return filename;
    }

  }

  /** Sorts key/value pairs in a sequence-format file. This class is no longer
   * used by Hadoop and will be removed in a later release.
   *
   * <p>For best performance, applications should make sure that the 
   * {@link RawComparator} that is used is efficient.
   */
  @Deprecated
  public static class Sorter {

    private final RawComparator comparator;
    private Writer.Option[] options;
    private final Configuration conf;
    private final FileContext fc;
    private int memory; // bytes
    private int factor; // merged per pass
    private final Serialization<?> keySerialization;
    private final Serialization<?> valueSerialization;

    private MergeSort mergeSort; //the implementation of merge sort
    
    private Path[] inFiles;                     // when merging or sorting

    private Path outFile;
    
    private CompressionType compressType;
    private CompressionCodec compressCodec;
    
    /**
     * Look at the first input file's header to figure out the compression for
     * the output.
     * @throws IOException
     */
    private void setCompressionType() throws IOException {
      if (inFiles == null || inFiles.length == 0) {
        return;
      }
      Reader reader = new Reader(conf, Reader.file(inFiles[0]),
                                 new Reader.OnlyHeaderOption());
      compressType = reader.getCompressionType();
      compressCodec = reader.getCompressionCodec();
      reader.close();
    }

    public static interface Option extends Writer.Option { }
    
    public static Option comparator(RawComparator value) {
      return new ComparatorOption(value);
    }

    private static class ComparatorOption extends Options.ComparatorOption 
                                          implements Option {
      private ComparatorOption(RawComparator value) {
        super(value);
      }
    }

    /**
     * Create a Sorter.
     * @param conf the configuration for the Sorter
     * @param options the options controlling the sort, in particular the
     *   comparator that will sort the data and the options to write the
     *   output SequenceFiles. Since the bytes are not deserialized during the
     *   sort, the serialization for keys and values of the inputs must match
     *   the options for writing the SequenceFiles.
     */
    public Sorter(Configuration conf, Writer.Option... options ) {
      this.options = options;
      this.conf = conf;
      this.memory = conf.getInt("io.sort.mb", 100) * 1024 * 1024;
      this.factor = conf.getInt("io.sort.factor", 100);
      try {
        fc = FileContext.getFileContext(conf);
      } catch (UnsupportedFileSystemException ex) {
        throw new IllegalArgumentException("can't load default filesystem", ex);
      }
      ComparatorOption compareOpt = Options.getOption(ComparatorOption.class, 
                                                      options);
      keySerialization = getSerialization(Writer.KeySerialization.class,
                                          Writer.KeyClassOption.class,
                                          options);
      valueSerialization = getSerialization(Writer.ValueSerialization.class,
                                            Writer.ValueClassOption.class,
                                            options);
      if (compareOpt == null) {
        comparator = keySerialization.getRawComparator();
      } else {
        comparator = compareOpt.getValue();
      }
    }

    private 
    Serialization<?> getSerialization(Class<? extends Writer.Option> serialOpt,
                                      Class<? extends Writer.Option> classOpt,
                                      Writer.Option[] options) {
      Options.SerializationOption serialOption = (Options.SerializationOption)
        Options.getOption(serialOpt, options);
      if (serialOption != null) {
        return serialOption.getValue();
      } else {
        Options.ClassOption classOption = (Options.ClassOption)
          Options.getOption(classOpt, options);
        if (classOption == null) {
          throw new IllegalArgumentException("Must specify either a " +
                                             "serializer, or "
                                             + "a class");
        }
        Class<?> cls = classOption.getValue();
        return SerializationFactory.getInstance(conf).
                 getSerializationByType(cls);
      }      
    }

    /**
     * Check to ensure the serialization of the input files matches the 
     * serialization we are using for the output. If they are not, it would
     * corrupt the outputs since we copy the keys and values as raw bytes.
     * @param reader the reader for the input file
     * @param filename the filename of the file
     * @throws IllegalArgumentException if the serialization is wrong
     */
    private void checkSerialization(Reader reader,
                                    Path filename) {
      if (!reader.getKeySerialization().equals(keySerialization)) {
        throw new IllegalArgumentException("key serialization of " +
                                           filename + 
                                           " does not match output" +
                                           " parameters");
      }
      if (!reader.getValueSerialization().equals(valueSerialization)) {
        throw new IllegalArgumentException("value serialization of " +
                                           filename + 
                                           " does not match output" +
                                           " parameters");
      }
    }

    /** Sort and merge files containing the named classes. 
     * @deprecated Use Sorter(Configuration, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Sorter(FileSystem fs, Class<? extends WritableComparable> keyClass,
                  Class valClass, Configuration conf)  {
      this(conf, Writer.keyClass(keyClass), Writer.valueClass(valClass));
    }

    /** Sort and merge using an arbitrary {@link RawComparator}. 
     * @deprecated Use Sorter(Configuration, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Sorter(FileSystem fs, RawComparator comparator, Class keyClass, 
                  Class valClass, Configuration conf) {
      this(conf, comparator(comparator), Writer.keyClass(keyClass),
           Writer.valueClass(valClass));
    }

    /** Sort and merge using an arbitrary {@link RawComparator}. 
     * @deprecated Use Sorter(Configuration, Option...) instead.
     */
    @SuppressWarnings("unchecked")
    @Deprecated
    public Sorter(FileSystem fs, RawComparator comparator, Class keyClass,
                  Class valClass, Configuration conf, Metadata metadata) {
      this(conf, comparator(comparator), Writer.keyClass(keyClass),
           Writer.valueClass(valClass), Writer.metadata(metadata));
    }

    /** Set the number of streams to merge at once.*/
    public void setFactor(int factor) { this.factor = factor; }

    /** Get the number of streams to merge at once.*/
    public int getFactor() { return factor; }

    /** Set the total amount of buffer memory, in bytes.*/
    public void setMemory(int memory) { this.memory = memory; }

    /** Get the total amount of buffer memory, in bytes.*/
    public int getMemory() { return memory; }

    /** Set the progressable object in order to report progress. 
     * @deprecated the progressable should be set when the Sorter is created.
     */
    @Deprecated
    public void setProgressable(Progressable progressable) {
      options = Options.prependOptions(options,
                                       Writer.progressable(progressable));
    }
    
    /** 
     * Perform a file sort from a set of input files into an output file.
     * @param inFiles the files to be sorted
     * @param outFile the sorted output file
     * @param deleteInput should the input files be deleted as they are read?
     */
    public void sort(Path[] inFiles, Path outFile,
                     boolean deleteInput) throws IOException {
      if (fc.util().exists(outFile)) {
        throw new IOException("already exists: " + outFile);
      }
      this.inFiles = inFiles;
      this.outFile = outFile;
      setCompressionType();

      int segments = sortPass(deleteInput);
      if (segments > 1) {
        mergePass(outFile.getParent());
      }
    }

    /** 
     * Perform a file sort from a set of input files and return an iterator.
     * @param inFiles the files to be sorted
     * @param tempDir the directory where temp files are created during sort
     * @param deleteInput should the input files be deleted as they are read?
     * @return iterator the RawKeyValueIterator
     */
    public RawKeyValueIterator sortAndIterate(Path[] inFiles, Path tempDir, 
                                              boolean deleteInput) throws IOException {
      Path outFile = new Path(tempDir + Path.SEPARATOR + "all.2");
      if (fc.util().exists(outFile)) {
        throw new IOException("already exists: " + outFile);
      }
      this.inFiles = inFiles;
      setCompressionType();

      //outFile will basically be used as prefix for temp files in the cases
      //where sort outputs multiple sorted segments. For the single segment
      //case, the outputFile itself will contain the sorted data for that
      //segment
      this.outFile = outFile;

      int segments = sortPass(deleteInput);
      if (segments > 1)
        return merge(outFile.suffix(".0"), outFile.suffix(".0.index"), 
                     tempDir);
      else if (segments == 1)
        return merge(new Path[]{outFile}, true, tempDir);
      else return null;
    }

    /**
     * The backwards compatible interface to sort.
     * @param inFile the input file to sort
     * @param outFile the sorted output file
     */
    public void sort(Path inFile, Path outFile) throws IOException {
      sort(new Path[]{inFile}, outFile, false);
    }
    
    private int sortPass(boolean deleteInput) throws IOException {
      LOG.debug("running sort pass");
      SortPass sortPass = new SortPass();         // make the SortPass
      mergeSort = new MergeSort(sortPass.new SeqFileComparator());
      try {
        return sortPass.run(deleteInput);         // run it
      } finally {
        sortPass.close();                         // close it
      }
    }

    private class SortPass {
      private int memoryLimit = memory/4;
      private int recordLimit = 1000000;
      
      private DataOutputBuffer rawKeys = new DataOutputBuffer();
      private byte[] rawBuffer;

      private int[] keyOffsets = new int[1024];
      private int[] pointers = new int[keyOffsets.length];
      private int[] pointersCopy = new int[keyOffsets.length];
      private int[] keyLengths = new int[keyOffsets.length];
      private ValueBytes[] rawValues = new ValueBytes[keyOffsets.length];
      
      private Reader in = null;
      private FSDataOutputStream out = null;
      private FSDataOutputStream indexOut = null;
      private Path outName;

      public int run(boolean deleteInput) throws IOException {
        int segments = 0;
        int currentFile = 0;
        boolean atEof = (currentFile >= inFiles.length);
        if (atEof) {
          return 0;
        }
        
        // Initialize
        in = new Reader(conf, Reader.file(inFiles[currentFile]));
        checkSerialization(in, inFiles[currentFile]);
        
        for (int i=0; i < rawValues.length; ++i) {
          rawValues[i] = null;
        }
        
        while (!atEof) {
          int count = 0;
          int bytesProcessed = 0;
          rawKeys.reset();
          while (!atEof && 
                 bytesProcessed < memoryLimit && count < recordLimit) {

            // Read a record into buffer
            // Note: Attempt to re-use 'rawValue' as far as possible
            int keyOffset = rawKeys.getLength();
            ValueBytes rawValue;
            if (count == keyOffsets.length || rawValues[count] == null) {
              rawValue = in.createValueBytes();
            } else {
              rawValue = rawValues[count];
            }
            int recordLength = in.nextRaw(rawKeys, rawValue);
            if (recordLength == -1) {
              in.close();
              if (deleteInput) {
                fc.delete(inFiles[currentFile], true);
              }
              currentFile += 1;
              atEof = currentFile >= inFiles.length;
              if (!atEof) {
                in = new Reader(conf, Reader.file(inFiles[currentFile]));
                checkSerialization(in, inFiles[currentFile]);
              } else {
                in = null;
              }
              continue;
            }

            int keyLength = rawKeys.getLength() - keyOffset;

            if (count == keyOffsets.length)
              grow();

            keyOffsets[count] = keyOffset;                // update pointers
            pointers[count] = count;
            keyLengths[count] = keyLength;
            rawValues[count] = rawValue;

            bytesProcessed += recordLength; 
            count++;
          }

          // buffer is full -- sort & flush it
          LOG.debug("flushing segment " + segments);
          rawBuffer = rawKeys.getData();
          sort(count);
          flush(count, bytesProcessed, segments==0 && atEof);
          segments++;
        }
        return segments;
      }

      public void close() throws IOException {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
        if (indexOut != null) {
          indexOut.close();
        }
      }

      private void grow() {
        int newLength = keyOffsets.length * 3 / 2;
        keyOffsets = grow(keyOffsets, newLength);
        pointers = grow(pointers, newLength);
        pointersCopy = new int[newLength];
        keyLengths = grow(keyLengths, newLength);
        rawValues = grow(rawValues, newLength);
      }

      private int[] grow(int[] old, int newLength) {
        int[] result = new int[newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        return result;
      }
      
      private ValueBytes[] grow(ValueBytes[] old, int newLength) {
        ValueBytes[] result = new ValueBytes[newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        for (int i=old.length; i < newLength; ++i) {
          result[i] = null;
        }
        return result;
      }

      private void flush(int count, int bytesProcessed, 
                         boolean done) throws IOException {
        if (out == null) {
          outName = done ? outFile : outFile.suffix(".0");
          out = fc.create(outName, EnumSet.of(CreateFlag.CREATE));
          if (!done) {
            indexOut = fc.create(outName.suffix(".index"),
                                 EnumSet.of(CreateFlag.CREATE));
          }
        }

        long segmentStart = out.getPos();
        Writer writer = 
          createWriter(conf, 
                       Options.prependOptions(options, 
                                              Writer.stream(out),
                                              Writer.compression(compressType,
                                                                 compressCodec)));
        
        if (!done) {
          writer.sync = null;                     // disable sync on temp files
        }

        for (int i = 0; i < count; i++) {         // write in sorted order
          int p = pointers[i];
          writer.appendRaw(rawBuffer, keyOffsets[p], keyLengths[p], rawValues[p]);
        }
        writer.close();
        
        if (!done) {
          // Save the segment length
          WritableUtils.writeVLong(indexOut, segmentStart);
          WritableUtils.writeVLong(indexOut, (out.getPos()-segmentStart));
          indexOut.flush();
        }
      }

      private void sort(int count) {
        System.arraycopy(pointers, 0, pointersCopy, 0, count);
        mergeSort.mergeSort(pointersCopy, pointers, 0, count);
      }
      class SeqFileComparator implements Comparator<IntWritable> {
        public int compare(IntWritable I, IntWritable J) {
          return comparator.compare(rawBuffer, keyOffsets[I.get()], 
                                    keyLengths[I.get()], rawBuffer, 
                                    keyOffsets[J.get()], keyLengths[J.get()]);
        }
      }
      
    } // SequenceFile.Sorter.SortPass

    /** The interface to iterate over raw keys/values of SequenceFiles. */
    public static interface RawKeyValueIterator {
      /** Gets the current raw key
       * @return DataOutputBuffer
       * @throws IOException
       */
      DataOutputBuffer getKey() throws IOException; 
      /** Gets the current raw value
       * @return ValueBytes 
       * @throws IOException
       */
      ValueBytes getValue() throws IOException; 
      /** Sets up the current key and value (for getKey and getValue)
       * @return true if there exists a key/value, false otherwise 
       * @throws IOException
       */
      boolean next() throws IOException;
      /** closes the iterator so that the underlying streams can be closed
       * @throws IOException
       */
      void close() throws IOException;
      /** Gets the Progress object; this has a float (0.0 - 1.0) 
       * indicating the bytes processed by the iterator so far
       */
      Progress getProgress();
    }    
    
    /**
     * Merges the list of segments of type <code>SegmentDescriptor</code>
     * @param segments the list of SegmentDescriptors
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIterator
     * @throws IOException
     */
    public RawKeyValueIterator merge(List <SegmentDescriptor> segments, 
                                     Path tmpDir) 
      throws IOException {
      // pass in object to report progress, if present
      MergeQueue mQueue = new MergeQueue(segments, tmpDir);
      return mQueue.merge();
    }

    /**
     * Merges the contents of files passed in Path[] using a max factor value
     * that is already set
     * @param inNames the array of path names
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public RawKeyValueIterator merge(Path [] inNames, boolean deleteInputs,
                                     Path tmpDir) 
      throws IOException {
      return merge(inNames, deleteInputs, 
                   (inNames.length < factor) ? inNames.length : factor,
                   tmpDir);
    }

    /**
     * Merges the contents of files passed in Path[]
     * @param inNames the array of path names
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @param factor the factor that will be used as the maximum merge fan-in
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public RawKeyValueIterator merge(Path [] inNames, boolean deleteInputs,
                                     int factor, Path tmpDir) 
      throws IOException {
      //get the segments from inNames
      ArrayList <SegmentDescriptor> a = new ArrayList <SegmentDescriptor>();
      for (int i = 0; i < inNames.length; i++) {
        SegmentDescriptor s = new SegmentDescriptor(0,
            fc.getFileStatus(inNames[i]).getLen(), inNames[i]);
        s.preserveInput(!deleteInputs);
        s.doSync();
        a.add(s);
      }
      this.factor = factor;
      MergeQueue mQueue = new MergeQueue(a, tmpDir);
      return mQueue.merge();
    }

    /**
     * Merges the contents of files passed in Path[]
     * @param inNames the array of path names
     * @param tempDir the directory for creating temp files during merge
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public RawKeyValueIterator merge(Path [] inNames, Path tempDir, 
                                     boolean deleteInputs) 
      throws IOException {
      //outFile will basically be used as prefix for temp files for the
      //intermediate merge outputs           
      this.outFile = new Path(tempDir + Path.SEPARATOR + "merged");
      //get the segments from inNames
      ArrayList <SegmentDescriptor> a = new ArrayList <SegmentDescriptor>();
      for (int i = 0; i < inNames.length; i++) {
        SegmentDescriptor s = new SegmentDescriptor(0,
            fc.getFileStatus(inNames[i]).getLen(), inNames[i]);
        s.preserveInput(!deleteInputs);
        s.doSync();
        a.add(s);
      }
      factor = (inNames.length < factor) ? inNames.length : factor;
      MergeQueue mQueue = new MergeQueue(a, tempDir);
      return mQueue.merge();
    }

    /**
     * Writes records from RawKeyValueIterator into a file represented by the 
     * passed writer
     * @param records the RawKeyValueIterator
     * @param writer the Writer created earlier 
     * @throws IOException
     */
    public void writeFile(RawKeyValueIterator records, Writer writer) 
      throws IOException {
      while(records.next()) {
        writer.appendRaw(records.getKey().getData(), 0, 
                         records.getKey().getLength(), records.getValue());
      }
      writer.sync();
    }
        
    /** Merge the provided files.
     * @param inFiles the array of input path names
     * @param outFile the final output file
     * @throws IOException
     */
    public void merge(Path[] inFiles, Path outFile) throws IOException {
      if (fc.util().exists(outFile)) {
        throw new IOException("already exists: " + outFile);
      }
      this.inFiles = inFiles;
      setCompressionType();
      RawKeyValueIterator r = merge(inFiles, false, outFile.getParent());
      Writer writer = 
        createWriter(conf, Options.prependOptions
                       (options, 
                        Writer.file(outFile),
                        Writer.compression(compressType, compressCodec)));
      writeFile(r, writer);

      writer.close();
    }

    /** sort calls this to generate the final merged output */
    private int mergePass(Path tmpDir) throws IOException {
      LOG.debug("running merge pass");
      Writer writer = 
        createWriter(conf, Options.prependOptions
                       (options, Writer.file(outFile),
                        Writer.compression(compressType, compressCodec)));
      RawKeyValueIterator r = merge(outFile.suffix(".0"), 
                                    outFile.suffix(".0.index"), tmpDir);
      writeFile(r, writer);

      writer.close();
      return 0;
    }

    /** Used by mergePass to merge the output of the sort
     * @param inName the name of the input file containing sorted segments
     * @param indexIn the offsets of the sorted segments
     * @param tmpDir the relative directory to store intermediate results in
     * @return RawKeyValueIterator
     * @throws IOException
     */
    private RawKeyValueIterator merge(Path inName, Path indexIn, Path tmpDir) 
      throws IOException {
      //get the segments from indexIn
      //we create a SegmentContainer so that we can track segments belonging to
      //inName and delete inName as soon as we see that we have looked at all
      //the contained segments during the merge process & hence don't need 
      //them anymore
      SegmentContainer container = new SegmentContainer(inName, indexIn);
      MergeQueue mQueue = new MergeQueue(container.getSegmentList(), tmpDir);
      return mQueue.merge();
    }
    
    /** This class implements the core of the merge logic */
    private class MergeQueue extends PriorityQueue<SegmentDescriptor>
      implements RawKeyValueIterator {
      private boolean compress;
      private boolean blockCompress;
      private DataOutputBuffer rawKey = new DataOutputBuffer();
      private ValueBytes rawValue;
      private long totalBytesProcessed;
      private float progPerByte;
      private Progress mergeProgress = new Progress();
      private Path tmpDir;
      private SegmentDescriptor minSegment;
      
      //a TreeMap used to store the segments sorted by size (segment offset and
      //segment path name is used to break ties between segments of same sizes)
      private Map<SegmentDescriptor, Void> sortedSegmentSizes =
        new TreeMap<SegmentDescriptor, Void>();
            
      public void addSegment(SegmentDescriptor stream) throws IOException {
        if (size() == 0) {
          compress = stream.in.isCompressed();
          blockCompress = stream.in.isBlockCompressed();
        } else if (compress != stream.in.isCompressed() || 
                   blockCompress != stream.in.isBlockCompressed()) {
          throw new IOException("All merged files must be compressed or not.");
        } 
        put(stream);
      }
      
      /**
       * A queue of file segments to merge
       * @param segments the file segments to merge
       * @param tmpDir a relative local directory to save intermediate files in
       */
      public MergeQueue(List <SegmentDescriptor> segments,
                        Path tmpDir) {
        int size = segments.size();
        for (int i = 0; i < size; i++) {
          sortedSegmentSizes.put(segments.get(i), null);
        }
        this.tmpDir = tmpDir;
      }
      protected boolean lessThan(Object a, Object b) {
        SegmentDescriptor msa = (SegmentDescriptor)a;
        SegmentDescriptor msb = (SegmentDescriptor)b;
        return comparator.compare(msa.getKey().getData(), 0, 
                                  msa.getKey().getLength(), msb.getKey().getData(), 0, 
                                  msb.getKey().getLength()) < 0;
      }
      public void close() throws IOException {
        SegmentDescriptor ms;                           // close inputs
        while ((ms = (SegmentDescriptor)pop()) != null) {
          ms.cleanup();
        }
        minSegment = null;
      }
      public DataOutputBuffer getKey() throws IOException {
        return rawKey;
      }
      public ValueBytes getValue() throws IOException {
        return rawValue;
      }
      public boolean next() throws IOException {
        if (size() == 0)
          return false;
        if (minSegment != null) {
          //minSegment is non-null for all invocations of next except the first
          //one. For the first invocation, the priority queue is ready for use
          //but for the subsequent invocations, first adjust the queue 
          adjustPriorityQueue(minSegment);
          if (size() == 0) {
            minSegment = null;
            return false;
          }
        }
        minSegment = (SegmentDescriptor)top();
        long startPos = minSegment.in.getPosition(); // Current position in stream
        //save the raw key reference
        rawKey = minSegment.getKey();
        //load the raw value. Re-use the existing rawValue buffer
        if (rawValue == null) {
          rawValue = minSegment.in.createValueBytes();
        }
        minSegment.nextRawValue(rawValue);
        long endPos = minSegment.in.getPosition(); // End position after reading value
        updateProgress(endPos - startPos);
        return true;
      }
      
      public Progress getProgress() {
        return mergeProgress; 
      }

      private void adjustPriorityQueue(SegmentDescriptor ms) throws IOException{
        long startPos = ms.in.getPosition(); // Current position in stream
        boolean hasNext = ms.nextRawKey();
        long endPos = ms.in.getPosition(); // End position after reading key
        updateProgress(endPos - startPos);
        if (hasNext) {
          adjustTop();
        } else {
          pop();
          ms.cleanup();
        }
      }

      private void updateProgress(long bytesProcessed) {
        totalBytesProcessed += bytesProcessed;
        if (progPerByte > 0) {
          mergeProgress.set(totalBytesProcessed * progPerByte);
        }
      }
      
      /** This is the single level merge that is called multiple times 
       * depending on the factor size and the number of segments
       * @return RawKeyValueIterator
       * @throws IOException
       */
      public RawKeyValueIterator merge() throws IOException {
        //create the MergeStreams from the sorted map created in the constructor
        //and dump the final output to a file
        int numSegments = sortedSegmentSizes.size();
        int origFactor = factor;
        int passNo = 1;
        LocalDirAllocator lDirAlloc = new LocalDirAllocator("io.seqfile.local.dir");
        do {
          //get the factor for this pass of merge
          factor = getPassFactor(passNo, numSegments);
          List<SegmentDescriptor> segmentsToMerge =
            new ArrayList<SegmentDescriptor>();
          int segmentsConsidered = 0;
          int numSegmentsToConsider = factor;
          while (true) {
            //extract the smallest 'factor' number of segment pointers from the 
            //TreeMap. Call cleanup on the empty segments (no key/value data)
            SegmentDescriptor[] mStream = 
              getSegmentDescriptors(numSegmentsToConsider);
            for (int i = 0; i < mStream.length; i++) {
              if (mStream[i].nextRawKey()) {
                segmentsToMerge.add(mStream[i]);
                segmentsConsidered++;
                // Count the fact that we read some bytes in calling nextRawKey()
                updateProgress(mStream[i].in.getPosition());
              }
              else {
                mStream[i].cleanup();
                numSegments--; //we ignore this segment for the merge
              }
            }
            //if we have the desired number of segments
            //or looked at all available segments, we break
            if (segmentsConsidered == factor || 
                sortedSegmentSizes.size() == 0) {
              break;
            }
              
            numSegmentsToConsider = factor - segmentsConsidered;
          }
          //feed the streams to the priority queue
          initialize(segmentsToMerge.size()); clear();
          for (int i = 0; i < segmentsToMerge.size(); i++) {
            addSegment(segmentsToMerge.get(i));
          }
          //if we have lesser number of segments remaining, then just return the
          //iterator, else do another single level merge
          if (numSegments <= factor) {
            //calculate the length of the remaining segments. Required for 
            //calculating the merge progress
            long totalBytes = 0;
            for (int i = 0; i < segmentsToMerge.size(); i++) {
              totalBytes += segmentsToMerge.get(i).segmentLength;
            }
            if (totalBytes != 0) //being paranoid
              progPerByte = 1.0f / (float)totalBytes;
            //reset factor to what it originally was
            factor = origFactor;
            return this;
          } else {
            //we want to spread the creation of temp files on multiple disks if 
            //available under the space constraints
            long approxOutputSize = 0; 
            for (SegmentDescriptor s : segmentsToMerge) {
              approxOutputSize += s.segmentLength + 
                                  ChecksumFileSystem.getApproxChkSumLength(
                                  s.segmentLength);
            }
            Path tmpFilename = 
              new Path(tmpDir, "intermediate").suffix("." + passNo);

            Path outputFile =  lDirAlloc.getLocalPathForWrite(
                                                tmpFilename.toString(),
                                                approxOutputSize, conf);
            LOG.debug("writing intermediate results to " + outputFile);
            Writer writer = 
              createWriter(conf, 
                           Options.prependOptions
                              (options, 
                               Writer.file(outputFile),
                               Writer.compression(compressType,
                                                  compressCodec)));
            writer.sync = null; //disable sync for temp files
            writeFile(this, writer);
            writer.close();
            
            //we finished one single level merge; now clean up the priority 
            //queue
            this.close();
            
            SegmentDescriptor tempSegment = 
              new SegmentDescriptor(0,
                  fc.getFileStatus(outputFile).getLen(), outputFile);
            //put the segment back in the TreeMap
            sortedSegmentSizes.put(tempSegment, null);
            numSegments = sortedSegmentSizes.size();
            passNo++;
          }
          //we are worried about only the first pass merge factor. So reset the 
          //factor to what it originally was
          factor = origFactor;
        } while(true);
      }
  
      //Hadoop-591
      public int getPassFactor(int passNo, int numSegments) {
        if (passNo > 1 || numSegments <= factor || factor == 1) 
          return factor;
        int mod = (numSegments - 1) % (factor - 1);
        if (mod == 0)
          return factor;
        return mod + 1;
      }
      
      /** Return (& remove) the requested number of segment descriptors from the
       * sorted map.
       */
      public SegmentDescriptor[] getSegmentDescriptors(int numDescriptors) {
        if (numDescriptors > sortedSegmentSizes.size())
          numDescriptors = sortedSegmentSizes.size();
        SegmentDescriptor[] SegmentDescriptors = 
          new SegmentDescriptor[numDescriptors];
        Iterator<SegmentDescriptor> iter = 
          sortedSegmentSizes.keySet().iterator();
        int i = 0;
        while (i < numDescriptors) {
          SegmentDescriptors[i++] = (SegmentDescriptor)iter.next();
          iter.remove();
        }
        return SegmentDescriptors;
      }
    } // SequenceFile.Sorter.MergeQueue

    /** This class defines a merge segment. This class can be subclassed to 
     * provide a customized cleanup method implementation. In this 
     * implementation, cleanup closes the file handle and deletes the file 
     */
    public class SegmentDescriptor implements Comparable<SegmentDescriptor> {
      
      long segmentOffset; //the start of the segment in the file
      long segmentLength; //the length of the segment
      Path segmentPathName; //the path name of the file containing the segment
      boolean ignoreSync = true; //set to true for temp files
      private Reader in = null; 
      private DataOutputBuffer rawKey = null; //this will hold the current key
      private boolean preserveInput = false; //delete input segment files?
      
      /** Constructs a segment
       * @param segmentOffset the offset of the segment in the file
       * @param segmentLength the length of the segment
       * @param segmentPathName the path name of the file containing the segment
       */
      public SegmentDescriptor (long segmentOffset, long segmentLength, 
                                Path segmentPathName) {
        this.segmentOffset = segmentOffset;
        this.segmentLength = segmentLength;
        this.segmentPathName = segmentPathName;
      }
      
      /** Do the sync checks */
      public void doSync() {ignoreSync = false;}
      
      /** Whether to delete the files when no longer needed */
      public void preserveInput(boolean preserve) {
        preserveInput = preserve;
      }

      public boolean shouldPreserveInput() {
        return preserveInput;
      }
      
      @Override
      public int compareTo(SegmentDescriptor that) {
        if (this.segmentLength != that.segmentLength) {
          return (this.segmentLength < that.segmentLength ? -1 : 1);
        }
        if (this.segmentOffset != that.segmentOffset) {
          return (this.segmentOffset < that.segmentOffset ? -1 : 1);
        }
        return (this.segmentPathName.toString()).
          compareTo(that.segmentPathName.toString());
      }

      public boolean equals(Object o) {
        if (!(o instanceof SegmentDescriptor)) {
          return false;
        }
        SegmentDescriptor that = (SegmentDescriptor)o;
        if (this.segmentLength == that.segmentLength &&
            this.segmentOffset == that.segmentOffset &&
            this.segmentPathName.toString().equals(
              that.segmentPathName.toString())) {
          return true;
        }
        return false;
      }

      public int hashCode() {
        return 37 * 17 + (int) (segmentOffset^(segmentOffset>>>32));
      }

      /** Fills up the rawKey object with the key returned by the Reader
       * @return true if there is a key returned; false, otherwise
       * @throws IOException
       */
      public boolean nextRawKey() throws IOException {
        if (in == null) {
          int bufferSize = getBufferSize(conf); 
          Reader reader = new Reader(conf,
                                     Reader.file(segmentPathName), 
                                     Reader.bufferSize(bufferSize),
                                     Reader.start(segmentOffset), 
                                     Reader.length(segmentLength));
          checkSerialization(reader, segmentPathName);
        
          //sometimes we ignore syncs especially for temp merge files
          if (ignoreSync) reader.ignoreSync();

          this.in = reader;
          rawKey = new DataOutputBuffer();
        }
        rawKey.reset();
        int keyLength = 
          in.nextRawKey(rawKey);
        return (keyLength >= 0);
      }

      /** Fills up the passed rawValue with the value corresponding to the key
       * read earlier
       * @param rawValue
       * @return the length of the value
       * @throws IOException
       */
      public int nextRawValue(ValueBytes rawValue) throws IOException {
        int valLength = in.nextRawValue(rawValue);
        return valLength;
      }
      
      /** Returns the stored rawKey */
      public DataOutputBuffer getKey() {
        return rawKey;
      }
      
      /** closes the underlying reader */
      private void close() throws IOException {
        this.in.close();
        this.in = null;
      }

      /** The default cleanup. Subclasses can override this with a custom 
       * cleanup 
       */
      public void cleanup() throws IOException {
        close();
        if (!preserveInput) {
          fc.delete(segmentPathName, true);
        }
      }
    } // SequenceFile.Sorter.SegmentDescriptor
    
    /** This class provisions multiple segments contained within a single
     *  file
     */
    private class LinkedSegmentsDescriptor extends SegmentDescriptor {

      SegmentContainer parentContainer = null;

      /** Constructs a segment
       * @param segmentOffset the offset of the segment in the file
       * @param segmentLength the length of the segment
       * @param segmentPathName the path name of the file containing the segment
       * @param parent the parent SegmentContainer that holds the segment
       */
      public LinkedSegmentsDescriptor (long segmentOffset, long segmentLength, 
                                       Path segmentPathName, SegmentContainer parent) {
        super(segmentOffset, segmentLength, segmentPathName);
        this.parentContainer = parent;
      }
      /** The default cleanup. Subclasses can override this with a custom 
       * cleanup 
       */
      public void cleanup() throws IOException {
        super.close();
        if (super.shouldPreserveInput()) return;
        parentContainer.cleanup();
      }
      
      public boolean equals(Object o) {
        if (!(o instanceof LinkedSegmentsDescriptor)) {
          return false;
        }
        return super.equals(o);
      }
    } //SequenceFile.Sorter.LinkedSegmentsDescriptor

    /** The class that defines a container for segments to be merged. Primarily
     * required to delete temp files as soon as all the contained segments
     * have been looked at */
    private class SegmentContainer {
      private int numSegmentsCleanedUp = 0; //track the no. of segment cleanups
      private int numSegmentsContained; //# of segments contained
      private Path inName; //input file from where segments are created
      
      //the list of segments read from the file
      private ArrayList <SegmentDescriptor> segments = 
        new ArrayList <SegmentDescriptor>();
      /** This constructor is there primarily to serve the sort routine that 
       * generates a single output file with an associated index file */
      public SegmentContainer(Path inName, Path indexIn) throws IOException {
        //get the segments from indexIn
        FSDataInputStream fsIndexIn = fc.open(indexIn);
        long end = fc.getFileStatus(indexIn).getLen();
        while (fsIndexIn.getPos() < end) {
          long segmentOffset = WritableUtils.readVLong(fsIndexIn);
          long segmentLength = WritableUtils.readVLong(fsIndexIn);
          Path segmentName = inName;
          segments.add(new LinkedSegmentsDescriptor(segmentOffset, 
                                                    segmentLength, segmentName, this));
        }
        fsIndexIn.close();
        fc.delete(indexIn, true);
        numSegmentsContained = segments.size();
        this.inName = inName;
      }

      public List <SegmentDescriptor> getSegmentList() {
        return segments;
      }
      public void cleanup() throws IOException {
        numSegmentsCleanedUp++;
        if (numSegmentsCleanedUp == numSegmentsContained) {
          fc.delete(inName, true);
        }
      }
    } //SequenceFile.Sorter.SegmentContainer

  } // SequenceFile.Sorter

} // SequenceFile
