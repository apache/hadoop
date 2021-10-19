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

package org.apache.hadoop.fs.s3a.select;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.AbortedException;
import com.amazonaws.services.s3.model.SelectObjectContentEvent;
import com.amazonaws.services.s3.model.SelectObjectContentEventVisitor;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import com.amazonaws.services.s3.model.SelectRecordsInputStream;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.s3a.Retries;
import org.apache.hadoop.fs.s3a.S3AReadOpContext;
import org.apache.hadoop.fs.s3a.S3ObjectAttributes;
import org.apache.hadoop.fs.s3a.statistics.S3AInputStreamStatistics;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.hadoop.fs.s3a.Invoker.once;
import static org.apache.hadoop.fs.s3a.S3AInputStream.validateReadahead;

/**
 * An input stream for S3 Select return values.
 * This is simply an end-to-end GET request, without any
 * form of seek or recovery from connectivity failures.
 *
 * Currently only seek and positioned read operations on the current
 * location are supported.
 *
 * The normal S3 input counters are updated by this stream.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SelectInputStream extends FSInputStream implements
    CanSetReadahead {

  private static final Logger LOG =
      LoggerFactory.getLogger(SelectInputStream.class);

  public static final String SEEK_UNSUPPORTED = "seek()";

  /**
   * Same set of arguments as for an S3AInputStream.
   */
  private final S3ObjectAttributes objectAttributes;

  /**
   * Tracks the current position.
   */
  private AtomicLong pos = new AtomicLong(0);

  /**
   * Closed flag.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * Did the read complete successfully?
   */
  private final AtomicBoolean completedSuccessfully = new AtomicBoolean(false);

  /**
   * Abortable response stream.
   * This is guaranteed to never be null.
   */
  private final SelectRecordsInputStream wrappedStream;

  private final String bucket;

  private final String key;

  private final String uri;

  private final S3AReadOpContext readContext;

  private final S3AInputStreamStatistics streamStatistics;

  private long readahead;

  /**
   * Create the stream.
   * The read attempt is initiated immediately.
   * @param readContext read context
   * @param objectAttributes object attributes from a HEAD request
   * @param selectResponse response from the already executed call
   * @throws IOException failure
   */
  @Retries.OnceTranslated
  public SelectInputStream(
      final S3AReadOpContext readContext,
      final S3ObjectAttributes objectAttributes,
      final SelectObjectContentResult selectResponse) throws IOException {
    Preconditions.checkArgument(isNotEmpty(objectAttributes.getBucket()),
        "No Bucket");
    Preconditions.checkArgument(isNotEmpty(objectAttributes.getKey()),
        "No Key");
    this.objectAttributes = objectAttributes;
    this.bucket = objectAttributes.getBucket();
    this.key = objectAttributes.getKey();
    this.uri = "s3a://" + this.bucket + "/" + this.key;
    this.readContext = readContext;
    this.readahead = readContext.getReadahead();
    this.streamStatistics = readContext.getS3AStatisticsContext()
        .newInputStreamStatistics();
    SelectRecordsInputStream stream = once(
        "S3 Select",
        uri,
        () -> selectResponse.getPayload()
            .getRecordsInputStream(new SelectObjectContentEventVisitor() {
              @Override
              public void visit(final SelectObjectContentEvent.EndEvent event) {
                LOG.debug("Completed successful S3 select read from {}", uri);
                completedSuccessfully.set(true);
              }
            }));
    this.wrappedStream = checkNotNull(stream);
    // this stream is already opened, so mark as such in the statistics.
    streamStatistics.streamOpened();
  }

  @Override
  public void close() throws IOException {
    long skipped = 0;
    boolean aborted = false;
    if (!closed.getAndSet(true)) {
      try {
        // set up for aborts.
        // if we know the available amount > readahead. Abort.
        //
        boolean shouldAbort = wrappedStream.available() > readahead;
        if (!shouldAbort) {
          // read our readahead range worth of data
          skipped = wrappedStream.skip(readahead);
          shouldAbort = wrappedStream.read() >= 0;
        }
        // now, either there is data left or not.
        if (shouldAbort) {
          // yes, more data. Abort and add this fact to the stream stats
          aborted = true;
          wrappedStream.abort();
        }
      } catch (IOException | AbortedException e) {
        LOG.debug("While closing stream", e);
      } finally {
        IOUtils.cleanupWithLogger(LOG, wrappedStream);
        streamStatistics.streamClose(aborted, skipped);
        streamStatistics.close();
        super.close();
      }
    }
  }

  /**
   * Verify that the input stream is open. Non blocking; this gives
   * the last state of the atomic {@link #closed} field.
   * @throws PathIOException if the connection is closed.
   */
  private void checkNotClosed() throws IOException {
    if (closed.get()) {
      throw new PathIOException(uri, FSExceptionMessages.STREAM_IS_CLOSED);
    }
  }

  @Override
  public int available() throws IOException {
    checkNotClosed();
    return wrappedStream.available();
  }

  @Override
  @Retries.OnceTranslated
  public synchronized long skip(final long n) throws IOException {
    checkNotClosed();
    long skipped = once("skip", uri, () -> wrappedStream.skip(n));
    pos.addAndGet(skipped);
    // treat as a forward skip for stats
    streamStatistics.seekForwards(skipped, skipped);
    return skipped;
  }

  @Override
  public long getPos() {
    return pos.get();
  }

  /**
   * Set the readahead.
   * @param readahead The readahead to use.  null means to use the default.
   */
  @Override
  public void setReadahead(Long readahead) {
    this.readahead = validateReadahead(readahead);
  }

  /**
   * Get the current readahead value.
   * @return the readahead
   */
  public long getReadahead() {
    return readahead;
  }

  /**
   * Read a byte. There's no attempt to recover, but AWS-SDK exceptions
   * such as {@code SelectObjectContentEventException} are translated into
   * IOExceptions.
   * @return a byte read or -1 for an end of file.
   * @throws IOException failure.
   */
  @Override
  @Retries.OnceTranslated
  public synchronized int read() throws IOException {
    checkNotClosed();
    int byteRead;
    try {
      byteRead = once("read()", uri, () -> wrappedStream.read());
    } catch (EOFException e) {
      // this could be one of: end of file, some IO failure
      if (completedSuccessfully.get()) {
        // read was successful
        return -1;
      } else {
        // the stream closed prematurely
        LOG.info("Reading of S3 Select data from {} failed before all results "
            + " were generated.", uri);
        streamStatistics.readException();
        throw new PathIOException(uri,
            "Read of S3 Select data did not complete");
      }
    }

    if (byteRead >= 0) {
      incrementBytesRead(1);
    }
    return byteRead;
  }

  @SuppressWarnings("NullableProblems")
  @Override
  @Retries.OnceTranslated
  public synchronized int read(final byte[] buf, final int off, final int len)
      throws IOException {
    checkNotClosed();
    validatePositionedReadArgs(pos.get(), buf, off, len);
    if (len == 0) {
      return 0;
    }

    int bytesRead;
    try {
      streamStatistics.readOperationStarted(pos.get(), len);
      bytesRead = wrappedStream.read(buf, off, len);
    } catch (EOFException e) {
      streamStatistics.readException();
      // the base implementation swallows EOFs.
      return -1;
    }

    incrementBytesRead(bytesRead);
    streamStatistics.readOperationCompleted(len, bytesRead);
    return bytesRead;
  }

  /**
   * Forward seeks are supported, but not backwards ones.
   * Forward seeks are implemented using read, so
   * means that long-distance seeks will be (literally) expensive.
   *
   * @param newPos new seek position.
   * @throws PathIOException Backwards seek attempted.
   * @throws EOFException attempt to seek past the end of the stream.
   * @throws IOException IO failure while skipping bytes
   */
  @Override
  @Retries.OnceTranslated
  public synchronized void seek(long newPos) throws IOException {
    long current = getPos();
    long distance = newPos - current;
    if (distance < 0) {
      throw unsupported(SEEK_UNSUPPORTED
          + " backwards from " + current + " to " + newPos);
    }
    if (distance == 0) {
      LOG.debug("ignoring seek to current position.");
    } else {
      // the complicated one: Forward seeking. Useful for split files.
      LOG.debug("Forward seek by reading {} bytes", distance);
      long bytesSkipped = 0;
      // read byte-by-byte, hoping that buffering will compensate for this.
      // doing it this way ensures that the seek stops at exactly the right
      // place. skip(len) can return a smaller value, at which point
      // it's not clear what to do.
      while(distance > 0) {
        int r = read();
        if (r == -1) {
          // reached an EOF too early
          throw new EOFException("Seek to " + newPos
              + " reached End of File at offset " + getPos());
        }
        distance--;
        bytesSkipped++;
      }
      // read has finished.
      streamStatistics.seekForwards(bytesSkipped, bytesSkipped);
    }
  }

  /**
   * Build an exception to raise when an operation is not supported here.
   * @param action action which is unsupported.
   * @return an exception to throw.
   */
  protected PathIOException unsupported(final String action) {
    return new PathIOException(
        String.format("s3a://%s/%s", bucket, key),
        action + " not supported");
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return false;
  }

  // Not supported.
  @Override
  public boolean markSupported() {
    return false;
  }

  @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
  @Override
  public void mark(int readLimit) {
    // Do nothing
  }

  @SuppressWarnings("NonSynchronizedMethodOverridesSynchronizedMethod")
  @Override
  public void reset() throws IOException {
    throw unsupported("Mark");
  }

  /**
   * Aborts the IO.
   */
  public void abort() {
    if (!closed.get()) {
      LOG.debug("Aborting");
      wrappedStream.abort();
    }
  }

  /**
   * Read at a specific position.
   * Reads at a position earlier than the current {@link #getPos()} position
   * will fail with a {@link PathIOException}. See {@link #seek(long)}.
   * Unlike the base implementation <i>And the requirements of the filesystem
   * specification, this updates the stream position as returned in
   * {@link #getPos()}.</i>
   * @param position offset in the stream.
   * @param buffer buffer to read in to.
   * @param offset offset within the buffer
   * @param length amount of data to read.
   * @return the result.
   * @throws PathIOException Backwards seek attempted.
   * @throws EOFException attempt to seek past the end of the stream.
   * @throws IOException IO failure while seeking in the stream or reading data.
   */
  @Override
  public int read(final long position,
      final byte[] buffer,
      final int offset,
      final int length)
      throws IOException {
    // maybe seek forwards to the position.
    seek(position);
    return read(buffer, offset, length);
  }

  /**
   * Increment the bytes read counter if there is a stats instance
   * and the number of bytes read is more than zero.
   * This also updates the {@link #pos} marker by the same value.
   * @param bytesRead number of bytes read
   */
  private void incrementBytesRead(long bytesRead) {
    if (bytesRead > 0) {
      pos.addAndGet(bytesRead);
    }
    streamStatistics.bytesRead(bytesRead);
    if (readContext.getStats() != null && bytesRead > 0) {
      readContext.getStats().incrementBytesRead(bytesRead);
    }
  }

  /**
   * Get the Stream statistics.
   * @return the statistics for this stream.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public S3AInputStreamStatistics getS3AStreamStatistics() {
    return streamStatistics;
  }

  /**
   * String value includes statistics as well as stream state.
   * <b>Important: there are no guarantees as to the stability
   * of this value.</b>
   * @return a string value for printing in logs/diagnostics
   */
  @Override
  @InterfaceStability.Unstable
  public String toString() {
    String s = streamStatistics.toString();
    synchronized (this) {
      final StringBuilder sb = new StringBuilder(
          "SelectInputStream{");
      sb.append(uri);
      sb.append("; state ").append(!closed.get() ? "open" : "closed");
      sb.append("; pos=").append(getPos());
      sb.append("; readahead=").append(readahead);
      sb.append('\n').append(s);
      sb.append('}');
      return sb.toString();
    }
  }
}
