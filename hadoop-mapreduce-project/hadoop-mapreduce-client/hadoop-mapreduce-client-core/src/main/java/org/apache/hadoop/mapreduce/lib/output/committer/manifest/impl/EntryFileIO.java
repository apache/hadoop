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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.functional.FutureIO;

import static java.util.Objects.requireNonNull;

/**
 * Read or write entry file.
 * This can be used to create a simple reader, or to create
 * a writer queue where different threads can queue data for
 * writing.
 * The entry file is a SequenceFile with KV = {NullWritable, FileEntry};
 */
public class EntryFileIO {

  private static final Logger LOG = LoggerFactory.getLogger(
      EntryFileIO.class);

  /** Configuration used to load filesystems. */
  private final Configuration conf;

  /**
   * ctor.
   * @param conf Configuration used to load filesystems
   */
  public EntryFileIO(final Configuration conf) {
    this.conf = conf;
  }

  /**
   * Create a writer to a local file.
   * @param file file
   * @return the writer
   * @throws IOException fail to open the file
   */
  public SequenceFile.Writer createWriter(File file) throws IOException {
    return createWriter(new Path(file.toURI()));
  }

  public SequenceFile.Writer createWriter(Path path) throws IOException {
    return SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(path),
        SequenceFile.Writer.keyClass(NullWritable.class),
        SequenceFile.Writer.valueClass(FileEntry.class));
  }


  /**
   * Reader is created with sequential reads.
   * @param file file
   * @return the reader
   * @throws IOException failure to open
   */
  public SequenceFile.Reader createReader(File file) throws IOException {
    return createReader(new Path(file.toURI()));
  }

  /**
   * Reader is created with sequential reads.
   * @param path path
   * @return the reader
   * @throws IOException failure to open
   */
  public SequenceFile.Reader createReader(Path path) throws IOException {
    return new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(path));
  }

  /**
   * Iterator to retrieve file entries from the sequence file.
   * Closeable: cast and invoke to close the reader.
   * @param reader reader;
   * @return iterator
   */
  public RemoteIterator<FileEntry> iterateOver(SequenceFile.Reader reader) {
    return new EntryIterator(reader);
  }

  /**
   * Create and start an entry writer.
   * @param writer writer
   * @param capacity queue capacity
   * @return the writer.
   */
  public EntryWriter launchEntryWriter(SequenceFile.Writer writer, int capacity) {
    final EntryWriter ew = new EntryWriter(writer, capacity);
    ew.start();
    return ew;
  }

  /**
   * Writer takes a list of entries at a time; queues for writing.
   * A special
   */
  public final class EntryWriter implements Closeable {

    private final SequenceFile.Writer writer;

    private final Queue<List<FileEntry>> queue;

    /**
     * stop flag.
     */
    private final AtomicBoolean stop = new AtomicBoolean(false);

    private final AtomicBoolean active = new AtomicBoolean(false);

    /**
     * Executor of writes.
     */
    private ExecutorService executor;

    /**
     * Future invoked.
     */
    private Future<Integer> future;

    /**
     * count of files opened; only updated in one thread
     * so volatile.
     */
    private volatile int count;

    /**
     * any failure.
     */
    private volatile IOException failure;

    /**
     * Create.
     * @param writer writer
     * @param capacity capacity.
     */
    private EntryWriter(SequenceFile.Writer writer, int capacity) {
      this.writer = writer;
      this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /**
     * Is the writer active?
     * @return true if the processor thread is live
     */
    public boolean isActive() {
      return active.get();
    }

    /**
     * Get count of files processed.
     * @return the count
     */
    public int getCount() {
      return count;
    }

    /**
     * Any failure.
     * @return any IOException caught when writing the output
     */
    public IOException getFailure() {
      return failure;
    }

    /**
     * Start the thread.
     */
    private void start() {
      Preconditions.checkState(executor == null, "already started");
      active.set(true);
      executor = HadoopExecutors.newSingleThreadExecutor();
      future = executor.submit(this::processor);
    }

    /**
     * Add a list of entries to the queue.
     * @param entries entries.
     * @return whether the queue worked.
     */
    public boolean enqueue(List<FileEntry> entries) {
      if (active.get()) {
        queue.add(entries);
        return false;
      } else {
        LOG.debug("Queue inactive; discarding {} entries", entries.size());
        return false;
      }
    }

    /**
     * Queue and process entries until done.
     * @return count of entries written.
     * @throws UncheckedIOException on write failure
     */
    private int processor() {
      int count = 0;
      while (!stop.get()) {
        queue.poll().forEach(this::append);
      }
      return count;
    }

    /**
     * write one entry.
     * @param entry entry to write
     * @throws UncheckedIOException on write failure
     */
    private void append(FileEntry entry) {
      if (failure != null) {
        try {
          writer.append(NullWritable.get(), entry);
          count++;
        } catch (IOException e) {
          failure = e;
          throw new UncheckedIOException(e);
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (stop.getAndSet(true)) {
        // already stopped
        return;
      }
      LOG.debug("Shutting down writer");
      // signal queue closure by
      // clearing the current list
      // and queue an empty list
      queue.clear();
      queue.add(new ArrayList<>());
      try {
        // wait for the op to finish.
        final int count = FutureIO.awaitFuture(future);
        LOG.debug("Processed {} files", count);
        // close the stream
      } finally {
        writer.close();
      }
    }
  }

  /**
   * Iterator to retrieve file entries from the sequence file.
   * Closeable.
   */
  private final class EntryIterator implements RemoteIterator<FileEntry>, Closeable {

    private final SequenceFile.Reader reader;

    private FileEntry fetched;

    private EntryIterator(final SequenceFile.Reader reader) {
      this.reader = requireNonNull(reader);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public boolean hasNext() throws IOException {
      return fetched != null || fetchNext();
    }

    private boolean fetchNext() throws IOException {
      FileEntry readBack = new FileEntry();
      if (reader.next(NullWritable.get(), readBack)) {
        fetched = readBack;
        return true;
      } else {
        fetched = null;
        return false;
      }
    }

    @Override
    public FileEntry next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final FileEntry r = fetched;
      fetched = null;
      return r;
    }
  }

}
