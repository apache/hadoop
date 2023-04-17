/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.test.AbstractHadoopTestBase;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.functional.RemoteIterators.foreach;

/**
 * Test {@link EntryFileIO}.
 */
public class TestEntryFileIO extends AbstractHadoopTestBase {

  public static final FileEntry ENTRY = new FileEntry("source", "dest", 100, "etag");

  /**
   * Entry file instance.
   */
  private EntryFileIO entryFileIO;

  private File entryFile;

  @Before
  public void setup() throws Exception {
    entryFileIO = new EntryFileIO(new Configuration());
    createEntryFile();
  }

  /**
   * Teardown.
   * @throws Exception on any failure
   */
  @After
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");
    if (getEntryFile() != null) {
      getEntryFile().delete();
    }
  }


  private void createEntryFile() throws IOException {
    setEntryFile(File.createTempFile("entry", ".seq"));
  }

  /**
   * reference to any temp file created.
   */
  private File getEntryFile() {
    return entryFile;
  }

  private void setEntryFile(File entryFile) {
    this.entryFile = entryFile;
  }


  /**
   * Create a file with one entry, then read it back
   * via all the mechanisms available.
   */
  @Test
  public void testCreateWriteReadFileOneEntry() throws Throwable {

    final FileEntry source = ENTRY;

    // do an explicit close to help isolate any failure.
    SequenceFile.Writer writer = createWriter();
    writer.append(NullWritable.get(), source);
    writer.flush();
    writer.close();

    FileEntry readBack = new FileEntry();
    try (SequenceFile.Reader reader = readEntryFile()) {
      reader.next(NullWritable.get(), readBack);
    }
    Assertions.assertThat(readBack)
        .describedAs("entry read back from sequence file")
        .isEqualTo(source);

    // now use the iterator to access it.
    final RemoteIterator<FileEntry> it =
        iterateOverEntryFile();
    List<FileEntry> files = new ArrayList<>();
    foreach(it, files::add);
    Assertions.assertThat(files)
        .describedAs("iteration over the entry file")
        .hasSize(1)
        .element(0)
        .isEqualTo(source);
    final EntryFileIO.EntryIterator et = (EntryFileIO.EntryIterator) it;
    Assertions.assertThat(et)
        .describedAs("entry iterator %s", et)
        .matches(p -> p.isClosed())
        .extracting(p -> p.getCount())
        .isEqualTo(1);
  }

  /**
   * Create a writer.
   * @return a writer
   * @throws IOException failure to create the file.
   */
  private SequenceFile.Writer createWriter() throws IOException {
    return entryFileIO.createWriter(getEntryFile());
  }

  /**
   * Create an iterator over the records in the (non empty) entry file.
   * @return an iterator over entries.
   * @throws IOException failure to open the file
   */
  private RemoteIterator<FileEntry> iterateOverEntryFile() throws IOException {
    return entryFileIO.iterateOver(readEntryFile());
  }

  /**
   * Create a reader for the (non empty) entry file.
   * @return a reader.
   * @throws IOException failure to open the file
   */
  private SequenceFile.Reader readEntryFile() throws IOException {
    assertEntryFileNonEmpty();

    return entryFileIO.createReader(getEntryFile());
  }

  /**
   * Create a file with one entry
   */
  @Test
  public void testCreateEmptyFile() throws Throwable {

    final File file = getEntryFile();

    entryFileIO.createWriter(file).close();

    // now use the iterator to access it.
    List<FileEntry> files = new ArrayList<>();
    Assertions.assertThat(foreach(iterateOverEntryFile(), files::add))
        .isEqualTo(0);
  }

  private void assertEntryFileNonEmpty() {
    Assertions.assertThat(getEntryFile().length())
        .describedAs("Length of file %s", getEntryFile())
        .isGreaterThan(0);
  }

  /**
   * Generate lots of data and write it.
   */
  @Test
  public void testLargeStreamingWrite() throws Throwable {

    // list of 100 entries at a time
    int listSize = 100;
    // and the number of block writes
    int writes = 100;
    List<FileEntry> list = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      list.add(new FileEntry("source" + i, "dest" + i, i, "etag-" + i));
    }
    // just for debugging/regression testing
    Assertions.assertThat(list).hasSize(listSize);
    int total = listSize * writes;

    try (EntryFileIO.EntryWriter out = entryFileIO.launchEntryWriter(createWriter(), 2)) {
      Assertions.assertThat(out.isActive())
          .describedAs("out.isActive in ()", out)
          .isTrue();
      for (int i = 0; i < writes; i++) {
        Assertions.assertThat(out.enqueue(list))
            .describedAs("enqueue of list")
            .isTrue();
      }
      out.close();
      out.maybeRaiseWriteException();
      Assertions.assertThat(out.isActive())
          .describedAs("out.isActive in ()", out)
          .isFalse();

      Assertions.assertThat(out.getCount())
          .describedAs("total elements written")
          .isEqualTo(total);
    }

    // now read it back
    AtomicInteger count = new AtomicInteger();
    foreach(iterateOverEntryFile(), e -> {
      final int elt = count.getAndIncrement();
      final int index = elt % listSize;
      Assertions.assertThat(e)
          .describedAs("element %d in file mapping to index %d", elt, index)
          .isEqualTo(list.get(index));
    });
    Assertions.assertThat(count.get())
        .describedAs("total elements read")
        .isEqualTo(total);
  }

  @Test
  public void testCreateInvalidWriter() throws Throwable {
    intercept(NullPointerException.class, () ->
        entryFileIO.launchEntryWriter(null, 1));
  }
  @Test
  public void testCreateInvalidWriterCapacity() throws Throwable {
    intercept(IllegalStateException.class, () ->
        entryFileIO.launchEntryWriter(null, 0));
  }

}
