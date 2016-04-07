/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Random;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Location;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * Byte arrays test case class using GZ compression codec, base class of none
 * and LZO compression classes.
 * 
 */
public class TestTFileByteArrays {
  private static String ROOT = GenericTestUtils.getTestDir().getAbsolutePath();
  private final static int BLOCK_SIZE = 512;
  private final static int BUF_SIZE = 64;
  private final static int K = 1024;
  protected boolean skip = false;

  private static final String KEY = "key";
  private static final String VALUE = "value";

  private FileSystem fs;
  private Configuration conf = new Configuration();
  private Path path;
  private FSDataOutputStream out;
  private Writer writer;

  private String compression = Compression.Algorithm.GZ.getName();
  private String comparator = "memcmp";
  private final String outputFile = getClass().getSimpleName();

  /*
   * pre-sampled numbers of records in one block, based on the given the
   * generated key and value strings. This is slightly different based on
   * whether or not the native libs are present.
   */
  private boolean usingNative = ZlibFactory.isNativeZlibLoaded(conf);
  private int records1stBlock = usingNative ? 5674 : 4480;
  private int records2ndBlock = usingNative ? 5574 : 4263;

  public void init(String compression, String comparator,
      int numRecords1stBlock, int numRecords2ndBlock) {
    init(compression, comparator);
    this.records1stBlock = numRecords1stBlock;
    this.records2ndBlock = numRecords2ndBlock;
  }
  
  public void init(String compression, String comparator) {
    this.compression = compression;
    this.comparator = comparator;
  }

  @Before
  public void setUp() throws IOException {
    path = new Path(ROOT, outputFile);
    fs = path.getFileSystem(conf);
    out = fs.create(path);
    writer = new Writer(out, BLOCK_SIZE, compression, comparator, conf);
  }

  @After
  public void tearDown() throws IOException {
    if (!skip)
      fs.delete(path, true);
  }

  @Test
  public void testNoDataEntry() throws IOException {
    if (skip) 
      return;
    closeOutput();

    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Assert.assertTrue(reader.isSorted());
    Scanner scanner = reader.createScanner();
    Assert.assertTrue(scanner.atEnd());
    scanner.close();
    reader.close();
  }

  @Test
  public void testOneDataEntry() throws IOException {
    if (skip)
      return;
    writeRecords(1);
    readRecords(1);

    checkBlockIndex(0, 0);
    readValueBeforeKey(0);
    readKeyWithoutValue(0);
    readValueWithoutKey(0);
    readKeyManyTimes(0);
  }

  @Test
  public void testTwoDataEntries() throws IOException {
    if (skip)
      return;
    writeRecords(2);
    readRecords(2);
  }

  /**
   * Fill up exactly one block.
   * 
   * @throws IOException
   */
  @Test
  public void testOneBlock() throws IOException {
    if (skip)
      return;
    // just under one block
    writeRecords(records1stBlock);
    readRecords(records1stBlock);
    // last key should be in the first block (block 0)
    checkBlockIndex(records1stBlock - 1, 0);
  }

  /**
   * One block plus one record.
   * 
   * @throws IOException
   */
  @Test
  public void testOneBlockPlusOneEntry() throws IOException {
    if (skip)
      return;
    writeRecords(records1stBlock + 1);
    readRecords(records1stBlock + 1);
    checkBlockIndex(records1stBlock - 1, 0);
    checkBlockIndex(records1stBlock, 1);
  }

  @Test
  public void testTwoBlocks() throws IOException {
    if (skip)
      return;
    writeRecords(records1stBlock + 5);
    readRecords(records1stBlock + 5);
    checkBlockIndex(records1stBlock + 4, 1);
  }

  @Test
  public void testThreeBlocks() throws IOException {
    if (skip) 
      return;
    writeRecords(2 * records1stBlock + 5);
    readRecords(2 * records1stBlock + 5);

    checkBlockIndex(2 * records1stBlock + 4, 2);
    // 1st key in file
    readValueBeforeKey(0);
    readKeyWithoutValue(0);
    readValueWithoutKey(0);
    readKeyManyTimes(0);
    // last key in file
    readValueBeforeKey(2 * records1stBlock + 4);
    readKeyWithoutValue(2 * records1stBlock + 4);
    readValueWithoutKey(2 * records1stBlock + 4);
    readKeyManyTimes(2 * records1stBlock + 4);

    // 1st key in mid block, verify block indexes then read
    checkBlockIndex(records1stBlock - 1, 0);
    checkBlockIndex(records1stBlock, 1);
    readValueBeforeKey(records1stBlock);
    readKeyWithoutValue(records1stBlock);
    readValueWithoutKey(records1stBlock);
    readKeyManyTimes(records1stBlock);

    // last key in mid block, verify block indexes then read
    checkBlockIndex(records1stBlock + records2ndBlock
        - 1, 1);
    checkBlockIndex(records1stBlock + records2ndBlock, 2);
    readValueBeforeKey(records1stBlock
        + records2ndBlock - 1);
    readKeyWithoutValue(records1stBlock
        + records2ndBlock - 1);
    readValueWithoutKey(records1stBlock
        + records2ndBlock - 1);
    readKeyManyTimes(records1stBlock + records2ndBlock
        - 1);

    // mid in mid block
    readValueBeforeKey(records1stBlock + 10);
    readKeyWithoutValue(records1stBlock + 10);
    readValueWithoutKey(records1stBlock + 10);
    readKeyManyTimes(records1stBlock + 10);
  }

  Location locate(Scanner scanner, byte[] key) throws IOException {
    if (scanner.seekTo(key) == true) {
      return scanner.currentLocation;
    }
    return scanner.endLocation;
  }
  
  @Test
  public void testLocate() throws IOException {
    if (skip)
      return;
    writeRecords(3 * records1stBlock);
    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner = reader.createScanner();
    locate(scanner, composeSortedKey(KEY, 2).getBytes());
    locate(scanner, composeSortedKey(KEY, records1stBlock - 1).getBytes());
    locate(scanner, composeSortedKey(KEY, records1stBlock).getBytes());
    Location locX = locate(scanner, "keyX".getBytes());
    Assert.assertEquals(scanner.endLocation, locX);
    scanner.close();
    reader.close();
  }

  @Test
  public void testFailureWriterNotClosed() throws IOException {
    if (skip)
      return;
    Reader reader = null;
    try {
      reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
      Assert.fail("Cannot read before closing the writer.");
    } catch (IOException e) {
      // noop, expecting exceptions
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Test
  public void testFailureWriteMetaBlocksWithSameName() throws IOException {
    if (skip)
      return;
    writer.append("keyX".getBytes(), "valueX".getBytes());

    // create a new metablock
    DataOutputStream outMeta =
        writer.prepareMetaBlock("testX", Compression.Algorithm.GZ.getName());
    outMeta.write(123);
    outMeta.write("foo".getBytes());
    outMeta.close();
    // add the same metablock
    try {
      writer.prepareMetaBlock("testX", Compression.Algorithm.GZ.getName());
      Assert.fail("Cannot create metablocks with the same name.");
    } catch (Exception e) {
      // noop, expecting exceptions
    }
    closeOutput();
  }

  @Test
  public void testFailureGetNonExistentMetaBlock() throws IOException {
    if (skip)
      return;
    writer.append("keyX".getBytes(), "valueX".getBytes());

    // create a new metablock
    DataOutputStream outMeta =
        writer.prepareMetaBlock("testX", Compression.Algorithm.GZ.getName());
    outMeta.write(123);
    outMeta.write("foo".getBytes());
    outMeta.close();
    closeOutput();

    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    DataInputStream mb = reader.getMetaBlock("testX");
    Assert.assertNotNull(mb);
    mb.close();
    try {
      DataInputStream mbBad = reader.getMetaBlock("testY");
      Assert.fail("Error on handling non-existent metablocks.");
    } catch (Exception e) {
      // noop, expecting exceptions
    }
    reader.close();
  }

  @Test
  public void testFailureWriteRecordAfterMetaBlock() throws IOException {
    if (skip)
      return;
    // write a key/value first
    writer.append("keyX".getBytes(), "valueX".getBytes());
    // create a new metablock
    DataOutputStream outMeta =
        writer.prepareMetaBlock("testX", Compression.Algorithm.GZ.getName());
    outMeta.write(123);
    outMeta.write("dummy".getBytes());
    outMeta.close();
    // add more key/value
    try {
      writer.append("keyY".getBytes(), "valueY".getBytes());
      Assert.fail("Cannot add key/value after start adding meta blocks.");
    } catch (Exception e) {
      // noop, expecting exceptions
    }
    closeOutput();
  }

  @Test
  public void testFailureReadValueManyTimes() throws IOException {
    if (skip)
      return;
    writeRecords(5);

    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner = reader.createScanner();

    byte[] vbuf = new byte[BUF_SIZE];
    int vlen = scanner.entry().getValueLength();
    scanner.entry().getValue(vbuf);
    Assert.assertEquals(new String(vbuf, 0, vlen), VALUE + 0);
    try {
      scanner.entry().getValue(vbuf);
      Assert.fail("Cannot get the value mlutiple times.");
    } catch (Exception e) {
      // noop, expecting exceptions
    }

    scanner.close();
    reader.close();
  }

  @Test
  public void testFailureBadCompressionCodec() throws IOException {
    if (skip)
      return;
    closeOutput();
    out = fs.create(path);
    try {
      writer = new Writer(out, BLOCK_SIZE, "BAD", comparator, conf);
      Assert.fail("Error on handling invalid compression codecs.");
    } catch (Exception e) {
      // noop, expecting exceptions
      // e.printStackTrace();
    }
  }

  @Test
  public void testFailureOpenEmptyFile() throws IOException {
    if (skip)
      return;
    closeOutput();
    // create an absolutely empty file
    path = new Path(fs.getWorkingDirectory(), outputFile);
    out = fs.create(path);
    out.close();
    try {
      new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
      Assert.fail("Error on handling empty files.");
    } catch (EOFException e) {
      // noop, expecting exceptions
    }
  }

  @Test
  public void testFailureOpenRandomFile() throws IOException {
    if (skip)
      return;
    closeOutput();
    // create an random file
    path = new Path(fs.getWorkingDirectory(), outputFile);
    out = fs.create(path);
    Random rand = new Random();
    byte[] buf = new byte[K];
    // fill with > 1MB data
    for (int nx = 0; nx < K + 2; nx++) {
      rand.nextBytes(buf);
      out.write(buf);
    }
    out.close();
    try {
      new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
      Assert.fail("Error on handling random files.");
    } catch (IOException e) {
      // noop, expecting exceptions
    }
  }

  @Test
  public void testFailureKeyLongerThan64K() throws IOException {
    if (skip)
      return;
    byte[] buf = new byte[64 * K + 1];
    Random rand = new Random();
    rand.nextBytes(buf);
    try {
      writer.append(buf, "valueX".getBytes());
    } catch (IndexOutOfBoundsException e) {
      // noop, expecting exceptions
    }
    closeOutput();
  }

  @Test
  public void testFailureOutOfOrderKeys() throws IOException {
    if (skip)
      return;
    try {
      writer.append("keyM".getBytes(), "valueM".getBytes());
      writer.append("keyA".getBytes(), "valueA".getBytes());
      Assert.fail("Error on handling out of order keys.");
    } catch (Exception e) {
      // noop, expecting exceptions
      // e.printStackTrace();
    }

    closeOutput();
  }

  @Test
  public void testFailureNegativeOffset() throws IOException {
    if (skip)
      return;
    try {
      writer.append("keyX".getBytes(), -1, 4, "valueX".getBytes(), 0, 6);
      Assert.fail("Error on handling negative offset.");
    } catch (Exception e) {
      // noop, expecting exceptions
    }
    closeOutput();
  }

  @Test
  public void testFailureNegativeOffset_2() throws IOException {
    if (skip)
      return;
    closeOutput();

    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner = reader.createScanner();
    try {
      scanner.lowerBound("keyX".getBytes(), -1, 4);
      Assert.fail("Error on handling negative offset.");
    } catch (Exception e) {
      // noop, expecting exceptions
    } finally {
      reader.close();
      scanner.close();
    }
    closeOutput();
  }

  @Test
  public void testFailureNegativeLength() throws IOException {
    if (skip)
      return;
    try {
      writer.append("keyX".getBytes(), 0, -1, "valueX".getBytes(), 0, 6);
      Assert.fail("Error on handling negative length.");
    } catch (Exception e) {
      // noop, expecting exceptions
    }
    closeOutput();
  }

  @Test
  public void testFailureNegativeLength_2() throws IOException {
    if (skip)
      return;
    closeOutput();

    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner = reader.createScanner();
    try {
      scanner.lowerBound("keyX".getBytes(), 0, -1);
      Assert.fail("Error on handling negative length.");
    } catch (Exception e) {
      // noop, expecting exceptions
    } finally {
      scanner.close();
      reader.close();
    }
    closeOutput();
  }

  @Test
  public void testFailureNegativeLength_3() throws IOException {
    if (skip)
      return;
    writeRecords(3);

    Reader reader =
        new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner = reader.createScanner();
    try {
      // test negative array offset
      try {
        scanner.seekTo("keyY".getBytes(), -1, 4);
        Assert.fail("Failed to handle negative offset.");
      } catch (Exception e) {
        // noop, expecting exceptions
      }

      // test negative array length
      try {
        scanner.seekTo("keyY".getBytes(), 0, -2);
        Assert.fail("Failed to handle negative key length.");
      } catch (Exception e) {
        // noop, expecting exceptions
      }
    } finally {
      reader.close();
      scanner.close();
    }
  }

  @Test
  public void testFailureCompressionNotWorking() throws IOException {
    if (skip)
      return;
    long rawDataSize = writeRecords(10 * records1stBlock, false);
    if (!compression.equalsIgnoreCase(Compression.Algorithm.NONE.getName())) {
      Assert.assertTrue(out.getPos() < rawDataSize);
    }
    closeOutput();
  }

  @Test
  public void testFailureFileWriteNotAt0Position() throws IOException {
    if (skip)
      return;
    closeOutput();
    out = fs.create(path);
    out.write(123);

    try {
      writer = new Writer(out, BLOCK_SIZE, compression, comparator, conf);
      Assert.fail("Failed to catch file write not at position 0.");
    } catch (Exception e) {
      // noop, expecting exceptions
    }
    closeOutput();
  }

  private long writeRecords(int count) throws IOException {
    return writeRecords(count, true);
  }

  private long writeRecords(int count, boolean close) throws IOException {
    long rawDataSize = writeRecords(writer, count);
    if (close) {
      closeOutput();
    }
    return rawDataSize;
  }

  static long writeRecords(Writer writer, int count) throws IOException {
    long rawDataSize = 0;
    int nx;
    for (nx = 0; nx < count; nx++) {
      byte[] key = composeSortedKey(KEY, nx).getBytes();
      byte[] value = (VALUE + nx).getBytes();
      writer.append(key, value);
      rawDataSize +=
          WritableUtils.getVIntSize(key.length) + key.length
              + WritableUtils.getVIntSize(value.length) + value.length;
    }
    return rawDataSize;
  }

  /**
   * Insert some leading 0's in front of the value, to make the keys sorted.
   * 
   * @param prefix
   * @param value
   * @return
   */
  static String composeSortedKey(String prefix, int value) {
    return String.format("%s%010d", prefix, value);
  }

  private void readRecords(int count) throws IOException {
    readRecords(fs, path, count, conf);
  }

  static void readRecords(FileSystem fs, Path path, int count,
      Configuration conf) throws IOException {
    Reader reader =
        new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner = reader.createScanner();

    try {
      for (int nx = 0; nx < count; nx++, scanner.advance()) {
        Assert.assertFalse(scanner.atEnd());
        // Assert.assertTrue(scanner.next());

        byte[] kbuf = new byte[BUF_SIZE];
        int klen = scanner.entry().getKeyLength();
        scanner.entry().getKey(kbuf);
        Assert.assertEquals(new String(kbuf, 0, klen), composeSortedKey(KEY,
            nx));

        byte[] vbuf = new byte[BUF_SIZE];
        int vlen = scanner.entry().getValueLength();
        scanner.entry().getValue(vbuf);
        Assert.assertEquals(new String(vbuf, 0, vlen), VALUE + nx);
      }

      Assert.assertTrue(scanner.atEnd());
      Assert.assertFalse(scanner.advance());
    } finally {
      scanner.close();
      reader.close();
    }
  }

  private void checkBlockIndex(int recordIndex, int blockIndexExpected) throws IOException {
    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner = reader.createScanner();
    scanner.seekTo(composeSortedKey(KEY, recordIndex).getBytes());
    Assert.assertEquals(blockIndexExpected, scanner.currentLocation
        .getBlockIndex());
    scanner.close();
    reader.close();
  }

  private void readValueBeforeKey(int recordIndex)
      throws IOException {
    Reader reader =
        new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner =
        reader.createScannerByKey(composeSortedKey(KEY, recordIndex)
            .getBytes(), null);

    try {
      byte[] vbuf = new byte[BUF_SIZE];
      int vlen = scanner.entry().getValueLength();
      scanner.entry().getValue(vbuf);
      Assert.assertEquals(new String(vbuf, 0, vlen), VALUE + recordIndex);

      byte[] kbuf = new byte[BUF_SIZE];
      int klen = scanner.entry().getKeyLength();
      scanner.entry().getKey(kbuf);
      Assert.assertEquals(new String(kbuf, 0, klen), composeSortedKey(KEY,
          recordIndex));
    } finally {
      scanner.close();
      reader.close();
    }
  }

  private void readKeyWithoutValue(int recordIndex)
      throws IOException {
    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);
    Scanner scanner =
        reader.createScannerByKey(composeSortedKey(KEY, recordIndex)
            .getBytes(), null);

    try {
      // read the indexed key
      byte[] kbuf1 = new byte[BUF_SIZE];
      int klen1 = scanner.entry().getKeyLength();
      scanner.entry().getKey(kbuf1);
      Assert.assertEquals(new String(kbuf1, 0, klen1), composeSortedKey(KEY,
          recordIndex));

      if (scanner.advance() && !scanner.atEnd()) {
        // read the next key following the indexed
        byte[] kbuf2 = new byte[BUF_SIZE];
        int klen2 = scanner.entry().getKeyLength();
        scanner.entry().getKey(kbuf2);
        Assert.assertEquals(new String(kbuf2, 0, klen2), composeSortedKey(KEY,
            recordIndex + 1));
      }
    } finally {
      scanner.close();
      reader.close();
    }
  }

  private void readValueWithoutKey(int recordIndex)
      throws IOException {
    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);

    Scanner scanner =
        reader.createScannerByKey(composeSortedKey(KEY, recordIndex)
            .getBytes(), null);

    byte[] vbuf1 = new byte[BUF_SIZE];
    int vlen1 = scanner.entry().getValueLength();
    scanner.entry().getValue(vbuf1);
    Assert.assertEquals(new String(vbuf1, 0, vlen1), VALUE + recordIndex);

    if (scanner.advance() && !scanner.atEnd()) {
      byte[] vbuf2 = new byte[BUF_SIZE];
      int vlen2 = scanner.entry().getValueLength();
      scanner.entry().getValue(vbuf2);
      Assert.assertEquals(new String(vbuf2, 0, vlen2), VALUE
          + (recordIndex + 1));
    }

    scanner.close();
    reader.close();
  }

  private void readKeyManyTimes(int recordIndex) throws IOException {
    Reader reader = new Reader(fs.open(path), fs.getFileStatus(path).getLen(), conf);

    Scanner scanner =
        reader.createScannerByKey(composeSortedKey(KEY, recordIndex)
            .getBytes(), null);

    // read the indexed key
    byte[] kbuf1 = new byte[BUF_SIZE];
    int klen1 = scanner.entry().getKeyLength();
    scanner.entry().getKey(kbuf1);
    Assert.assertEquals(new String(kbuf1, 0, klen1), composeSortedKey(KEY,
        recordIndex));

    klen1 = scanner.entry().getKeyLength();
    scanner.entry().getKey(kbuf1);
    Assert.assertEquals(new String(kbuf1, 0, klen1), composeSortedKey(KEY,
        recordIndex));

    klen1 = scanner.entry().getKeyLength();
    scanner.entry().getKey(kbuf1);
    Assert.assertEquals(new String(kbuf1, 0, klen1), composeSortedKey(KEY,
        recordIndex));

    scanner.close();
    reader.close();
  }

  private void closeOutput() throws IOException {
    if (writer != null) {
      writer.close();
      writer = null;
    }
    if (out != null) {
      out.close();
      out = null;
    }
  }
}
