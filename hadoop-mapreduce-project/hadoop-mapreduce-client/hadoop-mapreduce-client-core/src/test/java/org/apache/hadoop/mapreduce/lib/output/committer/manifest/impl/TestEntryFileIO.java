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
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.AbstractManifestCommitterTest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.util.functional.RemoteIterators.foreach;
import static org.apache.hadoop.util.functional.RemoteIterators.rangeExcludingIterator;

/**
 * Test {@link EntryFileIO}.
 */
public class TestEntryFileIO extends AbstractManifestCommitterTest {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestEntryFileIO.class);

  /**
   * Entry to save.
   */
  public static final FileEntry ENTRY = new FileEntry("source", "dest", 100, "etag");

  /**
   * Entry file instance.
   */
  private EntryFileIO entryFileIO;

  /**
   * Path to a test entry file.
   */
  private File entryFile;

  /**
   * Create an entry file during setup.
   */
  @Before
  public void setup() throws Exception {
    entryFileIO = new EntryFileIO(new Configuration());
    createEntryFile();
  }

  /**
   * Teardown deletes any entry file.
   * @throws Exception on any failure
   */
  @After
  public void teardown() throws Exception {
    Thread.currentThread().setName("teardown");
    if (getEntryFile() != null) {
      getEntryFile().delete();
    }
  }

  /**
   * Create a temp entry file and set the entryFile field to it.
   * @throws IOException creation failure
   */
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
   * Create a file with one entry.
   */
  @Test
  public void testCreateEmptyFile() throws Throwable {

    final File file = getEntryFile();

    entryFileIO.createWriter(file).close();

    // now use the iterator to access it.
    List<FileEntry> files = new ArrayList<>();
    Assertions.assertThat(foreach(iterateOverEntryFile(), files::add))
        .describedAs("Count of iterations over entries in an entry file with no entries")
        .isEqualTo(0);
  }

  private void assertEntryFileNonEmpty() {
    Assertions.assertThat(getEntryFile().length())
        .describedAs("Length of file %s", getEntryFile())
        .isGreaterThan(0);
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


  /**
   * Generate lots of data and write it.
   */
  @Test
  public void testLargeStreamingWrite() throws Throwable {

    // list of 100 entries at a time
    int listSize = 100;
    // and the number of block writes
    int writes = 100;
    List<FileEntry> list = buildEntryList(listSize);

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

  /**
   * Build an entry list.
   * @param listSize size of the list
   * @return a list of entries
   */
  private static List<FileEntry> buildEntryList(final int listSize) {
    List<FileEntry> list = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      list.add(new FileEntry("source" + i, "dest" + i, i, "etag-" + i));
    }
    // just for debugging/regression testing
    Assertions.assertThat(list).hasSize(listSize);
    return list;
  }

  /**
   * Write lists to the output, but the stream is going to fail after a
   * configured number of records have been written.
   * Verify that the (blocked) submitter is woken up
   * and that the exception was preserved for rethrowing.
   */
  @Test
  public void testFailurePropagation() throws Throwable {

    final int count = 4;
    final SequenceFile.Writer writer = spyWithFailingAppend(
        entryFileIO.createWriter(getEntryFile()), count);
    // list of 100 entries at a time
    // and the number of block writes
    List<FileEntry> list = buildEntryList(1);

    // small queue ensures the posting thread is blocked
    try (EntryFileIO.EntryWriter out = entryFileIO.launchEntryWriter(writer, 2)) {
      boolean valid = true;
      for (int i = 0; valid && i < count * 2; i++) {
        valid = out.enqueue(list);
      }
      LOG.info("queue to {} finished valid={}", out, valid);
      out.close();

      // verify the exception is as expected
      intercept(IOException.class, "mocked", () ->
          out.maybeRaiseWriteException());

      // and verify the count of invocations.
      Assertions.assertThat(out.getCount())
          .describedAs("process count of %s", count)
          .isEqualTo(count);
    }
  }

  /**
   * Spy on a writer with the append operation to fail after the given count of calls
   * is reached.
   * @param writer write.
   * @param count number of allowed append calls.
   * @return spied writer.
   * @throws IOException from the signature of the append() call mocked.
   */
  private static SequenceFile.Writer spyWithFailingAppend(final SequenceFile.Writer writer,
      final int count)
      throws IOException {
    AtomicLong limit = new AtomicLong(count);

    final SequenceFile.Writer spied = Mockito.spy(writer);
    Mockito.doAnswer((InvocationOnMock invocation) -> {
      final Writable k = invocation.getArgument(0);
      final Writable v = invocation.getArgument(1);
      if (limit.getAndDecrement() > 0) {
        writer.append(k, v);
      } else {
        throw new IOException("mocked");
      }
      return null;
    }).when(spied).append(Mockito.any(Writable.class), Mockito.any(Writable.class));
    return spied;
  }


  /**
   * Multithreaded writing.
   */
  @Test
  public void testParallelWrite() throws Throwable {

    // list of 100 entries at a time
    int listSize = 100;
    // and the number of block writes
    int attempts = 100;
    List<FileEntry> list = buildEntryList(listSize);

    int total = listSize * attempts;


    try (EntryFileIO.EntryWriter out = entryFileIO.launchEntryWriter(createWriter(), 20)) {
      TaskPool.foreach(rangeExcludingIterator(0, attempts))
          .executeWith(getSubmitter())
          .stopOnFailure()
          .run(l -> {
            out.enqueue(list);
          });
      out.close();
      out.maybeRaiseWriteException();

      Assertions.assertThat(out.getCount())
          .describedAs("total elements written")
          .isEqualTo(total);
    }

    // now read it back
    Assertions.assertThat(foreach(iterateOverEntryFile(), e -> { }))
        .describedAs("total elements read")
        .isEqualTo(total);
  }

}
