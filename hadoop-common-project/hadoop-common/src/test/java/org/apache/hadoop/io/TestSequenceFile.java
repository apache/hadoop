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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.avro.AvroReflectSerialization;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.*;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Support for flat files of binary key/value pairs. */
public class TestSequenceFile {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSequenceFile.class);

  private Configuration conf = new Configuration();

  /** Unit tests for SequenceFile. */
  @Test
  public void testZlibSequenceFile() throws Exception {
    LOG.info("Testing SequenceFile with DefaultCodec");
    compressedSeqFileTest(new DefaultCodec());
    LOG.info("Successfully tested SequenceFile with DefaultCodec");
  }

  @SuppressWarnings("deprecation")
  public void testSorterProperties() throws IOException {
    // Test to ensure that deprecated properties have no default
    // references anymore.
    Configuration config = new Configuration();
    assertNull("The deprecated sort memory property "
        + CommonConfigurationKeys.IO_SORT_MB_KEY
        + " must not exist in any core-*.xml files.",
        config.get(CommonConfigurationKeys.IO_SORT_MB_KEY));
    assertNull("The deprecated sort factor property "
        + CommonConfigurationKeys.IO_SORT_FACTOR_KEY
        + " must not exist in any core-*.xml files.",
        config.get(CommonConfigurationKeys.IO_SORT_FACTOR_KEY));

    // Test deprecated property honoring
    // Set different values for old and new property names
    // and compare which one gets loaded
    config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    config.setInt(CommonConfigurationKeys.IO_SORT_MB_KEY, 10);
    config.setInt(CommonConfigurationKeys.IO_SORT_FACTOR_KEY, 10);
    config.setInt(CommonConfigurationKeys.SEQ_IO_SORT_MB_KEY, 20);
    config.setInt(CommonConfigurationKeys.SEQ_IO_SORT_FACTOR_KEY, 20);
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(
        fs, Text.class, Text.class, config);
    assertEquals("Deprecated memory conf must be honored over newer property",
        10*1024*1024, sorter.getMemory());
    assertEquals("Deprecated factor conf must be honored over newer property",
        10, sorter.getFactor());

    // Test deprecated properties (graceful deprecation)
    config = new Configuration();
    fs = FileSystem.get(config);
    config.setInt(CommonConfigurationKeys.IO_SORT_MB_KEY, 10);
    config.setInt(CommonConfigurationKeys.IO_SORT_FACTOR_KEY, 10);
    sorter = new SequenceFile.Sorter(
        fs, Text.class, Text.class, config);
    assertEquals("Deprecated memory property "
        + CommonConfigurationKeys.IO_SORT_MB_KEY
        + " must get properly applied.",
        10*1024*1024, // In bytes
        sorter.getMemory());
    assertEquals("Deprecated sort factor property "
        + CommonConfigurationKeys.IO_SORT_FACTOR_KEY
        + " must get properly applied.",
        10, sorter.getFactor());

    // Test regular properties (graceful deprecation)
    config = new Configuration();
    fs = FileSystem.get(config);
    config.setInt(CommonConfigurationKeys.SEQ_IO_SORT_MB_KEY, 20);
    config.setInt(CommonConfigurationKeys.SEQ_IO_SORT_FACTOR_KEY, 20);
    sorter = new SequenceFile.Sorter(
        fs, Text.class, Text.class, config);
    assertEquals("Memory property "
        + CommonConfigurationKeys.SEQ_IO_SORT_MB_KEY
        + " must get properly applied if present.",
        20*1024*1024, // In bytes
        sorter.getMemory());
    assertEquals("Merge factor property "
        + CommonConfigurationKeys.SEQ_IO_SORT_FACTOR_KEY
        + " must get properly applied if present.",
        20, sorter.getFactor());
  }

  public void compressedSeqFileTest(CompressionCodec codec) throws Exception {
    int count = 1024 * 10;
    int megabytes = 1;
    int factor = 5;
    Path file = new Path(GenericTestUtils.getTempPath("test.seq"));
    Path recordCompressedFile = new Path(GenericTestUtils.getTempPath(
        "test.rc.seq"));
    Path blockCompressedFile = new Path(GenericTestUtils.getTempPath(
        "test.bc.seq"));
 
    int seed = new Random().nextInt();
    LOG.info("Seed = " + seed);

    FileSystem fs = FileSystem.getLocal(conf);
    try {
      // SequenceFile.Writer
      writeTest(fs, count, seed, file, CompressionType.NONE, null);
      readTest(fs, count, seed, file);

      sortTest(fs, count, megabytes, factor, false, file);
      checkSort(fs, count, seed, file);

      sortTest(fs, count, megabytes, factor, true, file);
      checkSort(fs, count, seed, file);

      mergeTest(fs, count, seed, file, CompressionType.NONE, false, 
                factor, megabytes);
      checkSort(fs, count, seed, file);

      mergeTest(fs, count, seed, file, CompressionType.NONE, true, 
                factor, megabytes);
      checkSort(fs, count, seed, file);
        
      // SequenceFile.RecordCompressWriter
      writeTest(fs, count, seed, recordCompressedFile, CompressionType.RECORD, 
                codec);
      readTest(fs, count, seed, recordCompressedFile);

      sortTest(fs, count, megabytes, factor, false, recordCompressedFile);
      checkSort(fs, count, seed, recordCompressedFile);

      sortTest(fs, count, megabytes, factor, true, recordCompressedFile);
      checkSort(fs, count, seed, recordCompressedFile);

      mergeTest(fs, count, seed, recordCompressedFile, 
                CompressionType.RECORD, false, factor, megabytes);
      checkSort(fs, count, seed, recordCompressedFile);

      mergeTest(fs, count, seed, recordCompressedFile, 
                CompressionType.RECORD, true, factor, megabytes);
      checkSort(fs, count, seed, recordCompressedFile);
        
      // SequenceFile.BlockCompressWriter
      writeTest(fs, count, seed, blockCompressedFile, CompressionType.BLOCK,
                codec);
      readTest(fs, count, seed, blockCompressedFile);

      sortTest(fs, count, megabytes, factor, false, blockCompressedFile);
      checkSort(fs, count, seed, blockCompressedFile);

      sortTest(fs, count, megabytes, factor, true, blockCompressedFile);
      checkSort(fs, count, seed, blockCompressedFile);

      mergeTest(fs, count, seed, blockCompressedFile, CompressionType.BLOCK, 
                false, factor, megabytes);
      checkSort(fs, count, seed, blockCompressedFile);

      mergeTest(fs, count, seed, blockCompressedFile, CompressionType.BLOCK, 
                true, factor, megabytes);
      checkSort(fs, count, seed, blockCompressedFile);

    } finally {
      fs.close();
    }
  }

  @SuppressWarnings("deprecation")
  private void writeTest(FileSystem fs, int count, int seed, Path file, 
                                CompressionType compressionType, CompressionCodec codec)
    throws IOException {
    fs.delete(file, true);
    LOG.info("creating " + count + " records with " + compressionType +
             " compression");
    SequenceFile.Writer writer = 
      SequenceFile.createWriter(fs, conf, file, 
                                RandomDatum.class, RandomDatum.class, compressionType, codec);
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      writer.append(key, value);
    }
    writer.close();
  }

  @SuppressWarnings("deprecation")
  private void readTest(FileSystem fs, int count, int seed, Path file)
    throws IOException {
    LOG.debug("reading " + count + " records");
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);

    RandomDatum k = new RandomDatum();
    RandomDatum v = new RandomDatum();
    DataOutputBuffer rawKey = new DataOutputBuffer();
    SequenceFile.ValueBytes rawValue = reader.createValueBytes();
    
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      try {
        if ((i%5) == 0) {
          // Testing 'raw' apis
          rawKey.reset();
          reader.nextRaw(rawKey, rawValue);
        } else {
          // Testing 'non-raw' apis 
          if ((i%2) == 0) {
            reader.next(k);
            reader.getCurrentValue(v);
          } else {
            reader.next(k, v);
          }
          
          // Check
          if (!k.equals(key))
            throw new RuntimeException("wrong key at " + i);
          if (!v.equals(value))
            throw new RuntimeException("wrong value at " + i);
        }
      } catch (IOException ioe) {
        LOG.info("Problem on row " + i);
        LOG.info("Expected key = " + key);
        LOG.info("Expected len = " + key.getLength());
        LOG.info("Actual key = " + k);
        LOG.info("Actual len = " + k.getLength());
        LOG.info("Expected value = " + value);
        LOG.info("Expected len = " + value.getLength());
        LOG.info("Actual value = " + v);
        LOG.info("Actual len = " + v.getLength());
        LOG.info("Key equals: " + k.equals(key));
        LOG.info("value equals: " + v.equals(value));
        throw ioe;
      }

    }
    reader.close();
  }


  private void sortTest(FileSystem fs, int count, int megabytes, 
                               int factor, boolean fast, Path file)
    throws IOException {
    fs.delete(new Path(file+".sorted"), true);
    SequenceFile.Sorter sorter = newSorter(fs, fast, megabytes, factor);
    LOG.debug("sorting " + count + " records");
    sorter.sort(file, file.suffix(".sorted"));
    LOG.info("done sorting " + count + " debug");
  }

  @SuppressWarnings("deprecation")
  private void checkSort(FileSystem fs, int count, int seed, Path file)
    throws IOException {
    LOG.info("sorting " + count + " records in memory for debug");
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    SortedMap<RandomDatum, RandomDatum> map =
      new TreeMap<RandomDatum, RandomDatum>();
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();
      map.put(key, value);
    }

    LOG.debug("checking order of " + count + " records");
    RandomDatum k = new RandomDatum();
    RandomDatum v = new RandomDatum();
    Iterator<Map.Entry<RandomDatum, RandomDatum>> iterator =
      map.entrySet().iterator();
    SequenceFile.Reader reader =
      new SequenceFile.Reader(fs, file.suffix(".sorted"), conf);
    for (int i = 0; i < count; i++) {
      Map.Entry<RandomDatum, RandomDatum> entry = iterator.next();
      RandomDatum key = entry.getKey();
      RandomDatum value = entry.getValue();

      reader.next(k, v);

      if (!k.equals(key))
        throw new RuntimeException("wrong key at " + i);
      if (!v.equals(value))
        throw new RuntimeException("wrong value at " + i);
    }

    reader.close();
    LOG.debug("sucessfully checked " + count + " records");
  }

  @SuppressWarnings("deprecation")
  private void mergeTest(FileSystem fs, int count, int seed, Path file, 
                                CompressionType compressionType,
                                boolean fast, int factor, int megabytes)
    throws IOException {

    LOG.debug("creating "+factor+" files with "+count/factor+" records");

    SequenceFile.Writer[] writers = new SequenceFile.Writer[factor];
    Path[] names = new Path[factor];
    Path[] sortedNames = new Path[factor];
    
    for (int i = 0; i < factor; i++) {
      names[i] = file.suffix("."+i);
      sortedNames[i] = names[i].suffix(".sorted");
      fs.delete(names[i], true);
      fs.delete(sortedNames[i], true);
      writers[i] = SequenceFile.createWriter(fs, conf, names[i], 
                                             RandomDatum.class, RandomDatum.class, compressionType);
    }

    RandomDatum.Generator generator = new RandomDatum.Generator(seed);

    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      writers[i%factor].append(key, value);
    }

    for (int i = 0; i < factor; i++)
      writers[i].close();

    for (int i = 0; i < factor; i++) {
      LOG.debug("sorting file " + i + " with " + count/factor + " records");
      newSorter(fs, fast, megabytes, factor).sort(names[i], sortedNames[i]);
    }

    LOG.info("merging " + factor + " files with " + count/factor + " debug");
    fs.delete(new Path(file+".sorted"), true);
    newSorter(fs, fast, megabytes, factor)
      .merge(sortedNames, file.suffix(".sorted"));
  }

  private SequenceFile.Sorter newSorter(FileSystem fs, 
                                               boolean fast,
                                               int megabytes, int factor) {
    SequenceFile.Sorter sorter = 
      fast
      ? new SequenceFile.Sorter(fs, new RandomDatum.Comparator(),
                                RandomDatum.class, RandomDatum.class, conf)
      : new SequenceFile.Sorter(fs, RandomDatum.class, RandomDatum.class, conf);
    sorter.setMemory(megabytes * 1024*1024);
    sorter.setFactor(factor);
    return sorter;
  }

  /** Unit tests for SequenceFile metadata. */
  @Test
  public void testSequenceFileMetadata() throws Exception {
    LOG.info("Testing SequenceFile with metadata");
    int count = 1024 * 10;
    CompressionCodec codec = new DefaultCodec();
    Path file = new Path(GenericTestUtils.getTempPath("test.seq.metadata"));
    Path sortedFile = new Path(GenericTestUtils.getTempPath(
        "test.sorted.seq.metadata"));
    Path recordCompressedFile = new Path(GenericTestUtils.getTempPath(
        "test.rc.seq.metadata"));
    Path blockCompressedFile = new Path(GenericTestUtils.getTempPath(
        "test.bc.seq.metadata"));
 
    FileSystem fs = FileSystem.getLocal(conf);
    SequenceFile.Metadata theMetadata = new SequenceFile.Metadata();
    theMetadata.set(new Text("name_1"), new Text("value_1"));
    theMetadata.set(new Text("name_2"), new Text("value_2"));
    theMetadata.set(new Text("name_3"), new Text("value_3"));
    theMetadata.set(new Text("name_4"), new Text("value_4"));
    
    int seed = new Random().nextInt();
    
    try {
      // SequenceFile.Writer
      writeMetadataTest(fs, count, seed, file, CompressionType.NONE, null, theMetadata);
      SequenceFile.Metadata aMetadata = readMetadata(fs, file);
      if (!theMetadata.equals(aMetadata)) {
        LOG.info("The original metadata:\n" + theMetadata.toString());
        LOG.info("The retrieved metadata:\n" + aMetadata.toString());
        throw new RuntimeException("metadata not match:  " + 1);
      }
      // SequenceFile.RecordCompressWriter
      writeMetadataTest(fs, count, seed, recordCompressedFile, CompressionType.RECORD, 
                        codec, theMetadata);
      aMetadata = readMetadata(fs, recordCompressedFile);
      if (!theMetadata.equals(aMetadata)) {
        LOG.info("The original metadata:\n" + theMetadata.toString());
        LOG.info("The retrieved metadata:\n" + aMetadata.toString());
        throw new RuntimeException("metadata not match:  " + 2);
      }
      // SequenceFile.BlockCompressWriter
      writeMetadataTest(fs, count, seed, blockCompressedFile, CompressionType.BLOCK,
                        codec, theMetadata);
      aMetadata =readMetadata(fs, blockCompressedFile);
      if (!theMetadata.equals(aMetadata)) {
        LOG.info("The original metadata:\n" + theMetadata.toString());
        LOG.info("The retrieved metadata:\n" + aMetadata.toString());
        throw new RuntimeException("metadata not match:  " + 3);
      }
      // SequenceFile.Sorter
      sortMetadataTest(fs, file, sortedFile, theMetadata);
      aMetadata = readMetadata(fs, recordCompressedFile);
      if (!theMetadata.equals(aMetadata)) {
        LOG.info("The original metadata:\n" + theMetadata.toString());
        LOG.info("The retrieved metadata:\n" + aMetadata.toString());
        throw new RuntimeException("metadata not match:  " + 4);
      }
    } finally {
      fs.close();
    }
    LOG.info("Successfully tested SequenceFile with metadata");
  }
  
  
  @SuppressWarnings("deprecation")
  private SequenceFile.Metadata readMetadata(FileSystem fs, Path file)
    throws IOException {
    LOG.info("reading file: " + file.toString());
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
    SequenceFile.Metadata meta = reader.getMetadata(); 
    reader.close();
    return meta;
  }

  @SuppressWarnings("deprecation")
  private void writeMetadataTest(FileSystem fs, int count, int seed, Path file, 
                                        CompressionType compressionType, CompressionCodec codec, SequenceFile.Metadata metadata)
    throws IOException {
    fs.delete(file, true);
    LOG.info("creating " + count + " records with metadata and with " + compressionType +
             " compression");
    SequenceFile.Writer writer = 
      SequenceFile.createWriter(fs, conf, file, 
                                RandomDatum.class, RandomDatum.class, compressionType, codec, null, metadata);
    RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for (int i = 0; i < count; i++) {
      generator.next();
      RandomDatum key = generator.getKey();
      RandomDatum value = generator.getValue();

      writer.append(key, value);
    }
    writer.close();
  }

  private void sortMetadataTest(FileSystem fs, Path unsortedFile, Path sortedFile, SequenceFile.Metadata metadata)
    throws IOException {
    fs.delete(sortedFile, true);
    LOG.info("sorting: " + unsortedFile + " to: " + sortedFile);
    final WritableComparator comparator = WritableComparator.get(RandomDatum.class);
    SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, comparator, RandomDatum.class, RandomDatum.class, conf, metadata);
    sorter.sort(new Path[] { unsortedFile }, sortedFile, false);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testClose() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
  
    // create a sequence file 1
    Path path1 = new Path(GenericTestUtils.getTempPath("test1.seq"));
    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path1,
        Text.class, NullWritable.class, CompressionType.BLOCK);
    writer.append(new Text("file1-1"), NullWritable.get());
    writer.append(new Text("file1-2"), NullWritable.get());
    writer.close();
  
    Path path2 = new Path(GenericTestUtils.getTempPath("test2.seq"));
    writer = SequenceFile.createWriter(fs, conf, path2, Text.class,
        NullWritable.class, CompressionType.BLOCK);
    writer.append(new Text("file2-1"), NullWritable.get());
    writer.append(new Text("file2-2"), NullWritable.get());
    writer.close();
  
    // Create a reader which uses 4 BuiltInZLibInflater instances
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path1, conf);
    // Returns the 4 BuiltInZLibInflater instances to the CodecPool
    reader.close();
    // The second close _could_ erroneously returns the same 
    // 4 BuiltInZLibInflater instances to the CodecPool again
    reader.close();
  
    // The first reader gets 4 BuiltInZLibInflater instances from the CodecPool
    SequenceFile.Reader reader1 = new SequenceFile.Reader(fs, path1, conf);
    // read first value from reader1
    Text text = new Text();
    reader1.next(text);
    assertEquals("file1-1", text.toString());
    
    // The second reader _could_ get the same 4 BuiltInZLibInflater 
    // instances from the CodePool as reader1
    SequenceFile.Reader reader2 = new SequenceFile.Reader(fs, path2, conf);
    
    // read first value from reader2
    reader2.next(text);
    assertEquals("file2-1", text.toString());
    // read second value from reader1
    reader1.next(text);
    assertEquals("file1-2", text.toString());
    // read second value from reader2 (this throws an exception)
    reader2.next(text);
    assertEquals("file2-2", text.toString());
  
    assertFalse(reader1.next(text));
    assertFalse(reader2.next(text));
  }

  /**
   * Test that makes sure the FileSystem passed to createWriter
   * @throws Exception
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testCreateUsesFsArg() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    FileSystem spyFs = Mockito.spy(fs);
    Path p = new Path(GenericTestUtils.getTempPath("testCreateUsesFSArg.seq"));
    SequenceFile.Writer writer = SequenceFile.createWriter(
        spyFs, conf, p, NullWritable.class, NullWritable.class);
    writer.close();
    Mockito.verify(spyFs).getDefaultReplication(p);
  }

  private static class TestFSDataInputStream extends FSDataInputStream {
    private boolean closed = false;

    private TestFSDataInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }

    public boolean isClosed() {
      return closed;
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testCloseForErroneousSequenceFile()
    throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);

    // create an empty file (which is not a valid sequence file)
    Path path = new Path(GenericTestUtils.getTempPath("broken.seq"));
    fs.create(path).close();

    // try to create SequenceFile.Reader
    final TestFSDataInputStream[] openedFile = new TestFSDataInputStream[1];
    try {
      new SequenceFile.Reader(fs, path, conf) {
        // this method is called by the SequenceFile.Reader constructor, overwritten, so we can access the opened file
        @Override
        protected FSDataInputStream openFile(FileSystem fs, Path file, int bufferSize, long length) throws IOException {
          final InputStream in = super.openFile(fs, file, bufferSize, length);
          openedFile[0] = new TestFSDataInputStream(in);
          return openedFile[0];
        }
      };
      fail("IOException expected.");
    } catch (IOException expected) {}

    assertNotNull(path + " should have been opened.", openedFile[0]);
    assertTrue("InputStream for " + path + " should have been closed.", openedFile[0].isClosed());
  }

  /**
   * Test to makes sure zero length sequence file is handled properly while
   * initializing.
   */
  @Test
  public void testInitZeroLengthSequenceFile() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);

    // create an empty file (which is not a valid sequence file)
    Path path = new Path(GenericTestUtils.getTempPath("zerolength.seq"));
    fs.create(path).close();

    try {
      new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
      fail("IOException expected.");
    } catch (IOException expected) {
      assertTrue(expected instanceof EOFException);
    }
  }

   /**
   * Test that makes sure createWriter succeeds on a file that was 
   * already created
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  @Test
  public void testCreateWriterOnExistingFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path name = new Path(new Path(GenericTestUtils.getTempPath(
        "createWriterOnExistingFile")), "file");

    fs.create(name);
    SequenceFile.createWriter(fs, conf, name, RandomDatum.class,
        RandomDatum.class, 512, (short) 1, 4096, false,
        CompressionType.NONE, null, new Metadata());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testRecursiveSeqFileCreate() throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    Path name = new Path(new Path(GenericTestUtils.getTempPath(
        "recursiveCreateDir")), "file");
    boolean createParent = false;

    try {
      SequenceFile.createWriter(fs, conf, name, RandomDatum.class,
          RandomDatum.class, 512, (short) 1, 4096, createParent,
          CompressionType.NONE, null, new Metadata());
      fail("Expected an IOException due to missing parent");
    } catch (IOException ioe) {
      // Expected
    }

    createParent = true;
    SequenceFile.createWriter(fs, conf, name, RandomDatum.class,
        RandomDatum.class, 512, (short) 1, 4096, createParent,
        CompressionType.NONE, null, new Metadata());
    // should succeed, fails if exception thrown
  }

  @Test
  public void testSerializationAvailability() throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(GenericTestUtils.getTempPath(
        "serializationAvailability"));
    // Check if any serializers aren't found.
    try {
      SequenceFile.createWriter(
          conf,
          SequenceFile.Writer.file(path),
          SequenceFile.Writer.keyClass(String.class),
          SequenceFile.Writer.valueClass(NullWritable.class));
      // Note: This may also fail someday if JavaSerialization
      // is activated by default.
      fail("Must throw IOException for missing serializer for the Key class");
    } catch (IOException e) {
      assertTrue(e.getMessage().startsWith(
        "Could not find a serializer for the Key class: '" +
            String.class.getName() + "'."));
    }
    try {
      SequenceFile.createWriter(
          conf,
          SequenceFile.Writer.file(path),
          SequenceFile.Writer.keyClass(NullWritable.class),
          SequenceFile.Writer.valueClass(String.class));
      // Note: This may also fail someday if JavaSerialization
      // is activated by default.
      fail("Must throw IOException for missing serializer for the Value class");
    } catch (IOException e) {
      assertTrue(e.getMessage().startsWith(
        "Could not find a serializer for the Value class: '" +
            String.class.getName() + "'."));
    }

    // Write a simple file to test deserialization failures with
    writeTest(FileSystem.get(conf), 1, 1, path, CompressionType.NONE, null);

    // Remove Writable serializations, to enforce error.
    conf.setStrings(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY,
        AvroReflectSerialization.class.getName());

    // Now check if any deserializers aren't found.
    try {
      new SequenceFile.Reader(
          conf,
          SequenceFile.Reader.file(path));
      fail("Must throw IOException for missing deserializer for the Key class");
    } catch (IOException e) {
      assertTrue(e.getMessage().startsWith(
        "Could not find a deserializer for the Key class: '" +
            RandomDatum.class.getName() + "'."));
    }
  }

  @Test
  public void testSequenceFileWriter() throws Exception {
    Configuration conf = new Configuration();
    // This test only works with Raw File System and not Local File System
    FileSystem fs = FileSystem.getLocal(conf).getRaw();
    Path p = new Path(GenericTestUtils
      .getTempPath("testSequenceFileWriter.seq"));
    try(SequenceFile.Writer writer = SequenceFile.createWriter(
            fs, conf, p, LongWritable.class, Text.class)) {
      Assertions.assertThat(writer.hasCapability
        (StreamCapabilities.HSYNC)).isEqualTo(true);
      Assertions.assertThat(writer.hasCapability(
        StreamCapabilities.HFLUSH)).isEqualTo(true);
      LongWritable key = new LongWritable();
      key.set(1);
      Text value = new Text();
      value.set("somevalue");
      writer.append(key, value);
      writer.flush();
      writer.hflush();
      writer.hsync();
      Assertions.assertThat(fs.getFileStatus(p).getLen()).isGreaterThan(0);
    }
  }

  /** For debugging and testing. */
  public static void main(String[] args) throws Exception {
    int count = 1024 * 1024;
    int megabytes = 1;
    int factor = 10;
    boolean create = true;
    boolean rwonly = false;
    boolean check = false;
    boolean fast = false;
    boolean merge = false;
    String compressType = "NONE";
    String compressionCodec = "org.apache.hadoop.io.compress.DefaultCodec";
    Path file = null;
    int seed = new Random().nextInt();

    String usage = "Usage: testsequencefile " +
      "[-count N] " + 
      "[-seed #] [-check] [-compressType <NONE|RECORD|BLOCK>] " + 
      "-codec <compressionCodec> " + 
      "[[-rwonly] | {[-megabytes M] [-factor F] [-nocreate] [-fast] [-merge]}] " +
      " file";
    if (args.length == 0) {
      System.err.println(usage);
      System.exit(-1);
    }
    
    FileSystem fs = null;
    try {
      for (int i=0; i < args.length; ++i) {       // parse command line
        if (args[i] == null) {
          continue;
        } else if (args[i].equals("-count")) {
          count = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-megabytes")) {
          megabytes = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-factor")) {
          factor = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-seed")) {
          seed = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-rwonly")) {
          rwonly = true;
        } else if (args[i].equals("-nocreate")) {
          create = false;
        } else if (args[i].equals("-check")) {
          check = true;
        } else if (args[i].equals("-fast")) {
          fast = true;
        } else if (args[i].equals("-merge")) {
          merge = true;
        } else if (args[i].equals("-compressType")) {
          compressType = args[++i];
        } else if (args[i].equals("-codec")) {
          compressionCodec = args[++i];
        } else {
          // file is required parameter
          file = new Path(args[i]);
        }
      }
        
      TestSequenceFile test = new TestSequenceFile();
      
      fs = file.getFileSystem(test.conf);

      LOG.info("count = " + count);
      LOG.info("megabytes = " + megabytes);
      LOG.info("factor = " + factor);
      LOG.info("create = " + create);
      LOG.info("seed = " + seed);
      LOG.info("rwonly = " + rwonly);
      LOG.info("check = " + check);
      LOG.info("fast = " + fast);
      LOG.info("merge = " + merge);
      LOG.info("compressType = " + compressType);
      LOG.info("compressionCodec = " + compressionCodec);
      LOG.info("file = " + file);

      if (rwonly && (!create || merge || fast)) {
        System.err.println(usage);
        System.exit(-1);
      }

      CompressionType compressionType = 
        CompressionType.valueOf(compressType);
      CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(
                                                                             test.conf.getClassByName(compressionCodec), 
                                                                             test.conf);

      if (rwonly || (create && !merge)) {
        test.writeTest(fs, count, seed, file, compressionType, codec);
        test.readTest(fs, count, seed, file);
      }

      if (!rwonly) {
        if (merge) {
          test.mergeTest(fs, count, seed, file, compressionType, 
                    fast, factor, megabytes);
        } else {
          test.sortTest(fs, count, megabytes, factor, fast, file);
        }
      }
    
      if (check) {
        test.checkSort(fs, count, seed, file);
      }
    } finally {
      if (fs != null) {
        fs.close();
      }
    }
  }
}
