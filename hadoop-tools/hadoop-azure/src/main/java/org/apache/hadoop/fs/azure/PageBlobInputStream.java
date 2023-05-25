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

package org.apache.hadoop.fs.azure;

import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.PAGE_DATA_SIZE;
import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.PAGE_HEADER_SIZE;
import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.PAGE_SIZE;
import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.toShort;
import static org.apache.hadoop.fs.azure.PageBlobFormatHelpers.withMD5Checking;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.azure.StorageInterface.CloudPageBlobWrapper;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.PageRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An input stream that reads file data from a page blob stored
 * using ASV's custom format.
 */

final class PageBlobInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(PageBlobInputStream.class);

  // The blob we're reading from.
  private final CloudPageBlobWrapper blob;
  // The operation context to use for storage requests.
  private final OperationContext opContext;
  // The number of pages remaining to be read from the server.
  private long numberOfPagesRemaining;
  // The current byte offset to start reading from the server next,
  // equivalent to (total number of pages we've read) * (page size).
  private long currentOffsetInBlob;
  // The buffer holding the current data we last read from the server.
  private byte[] currentBuffer;
  // The current byte offset we're at in the buffer.
  private int currentBufferOffset;
  // The current buffer length
  private int currentBufferLength;
  // Maximum number of pages to get per any one request.
  private static final int MAX_PAGES_PER_DOWNLOAD =
      4 * 1024 * 1024 / PAGE_SIZE;
  // Whether the stream has been closed.
  private boolean closed = false;
  // Total stream size, or -1 if not initialized.
  long pageBlobSize = -1;
  // Current position in stream of valid data.
  long filePosition = 0;

  /**
   * Helper method to extract the actual data size of a page blob.
   * This typically involves 2 service requests (one for page ranges, another
   * for the last page's data).
   *
   * @param blob The blob to get the size from.
   * @param opContext The operation context to use for the requests.
   * @return The total data size of the blob in bytes.
   * @throws IOException If the format is corrupt.
   * @throws StorageException If anything goes wrong in the requests.
   */
  public static long getPageBlobDataSize(CloudPageBlobWrapper blob,
      OperationContext opContext) throws IOException, StorageException {
    // Get the page ranges for the blob. There should be one range starting
    // at byte 0, but we tolerate (and ignore) ranges after the first one.
    ArrayList<PageRange> pageRanges =
        blob.downloadPageRanges(new BlobRequestOptions(), opContext);
    if (pageRanges.size() == 0) {
      return 0;
    }
    if (pageRanges.get(0).getStartOffset() != 0) {
      // Not expected: we always upload our page blobs as a contiguous range
      // starting at byte 0.
      throw badStartRangeException(blob, pageRanges.get(0));
    }
    long totalRawBlobSize = pageRanges.get(0).getEndOffset() + 1;

    // Get the last page.
    long lastPageStart = totalRawBlobSize - PAGE_SIZE;
    ByteArrayOutputStream baos = 
        new ByteArrayOutputStream(PageBlobFormatHelpers.PAGE_SIZE);
    blob.downloadRange(lastPageStart, PAGE_SIZE, baos,
        new BlobRequestOptions(), opContext);

    byte[] lastPage = baos.toByteArray();
    short lastPageSize = getPageSize(blob, lastPage, 0);
    long totalNumberOfPages = totalRawBlobSize / PAGE_SIZE;
    return (totalNumberOfPages - 1) * PAGE_DATA_SIZE + lastPageSize;
  }

  /**
   * Constructs a stream over the given page blob.
   */
  public PageBlobInputStream(CloudPageBlobWrapper blob,
      OperationContext opContext)
      throws IOException {
    this.blob = blob;
    this.opContext = opContext;
    ArrayList<PageRange> allRanges;
    try {
      allRanges =
          blob.downloadPageRanges(new BlobRequestOptions(), opContext);
    } catch (StorageException e) {
      throw new IOException(e);
    }
    if (allRanges.size() > 0) {
      if (allRanges.get(0).getStartOffset() != 0) {
        throw badStartRangeException(blob, allRanges.get(0));
      }
      if (allRanges.size() > 1) {
        LOG.warn(String.format(
            "Blob %s has %d page ranges beyond the first range. " 
            + "Only reading the first range.",
            blob.getUri(), allRanges.size() - 1));
      }
      numberOfPagesRemaining =
          (allRanges.get(0).getEndOffset() + 1) / PAGE_SIZE;
    } else {
      numberOfPagesRemaining = 0;
    }
  }

  /** Return the size of the remaining available bytes
   * if the size is less than or equal to {@link Integer#MAX_VALUE},
   * otherwise, return {@link Integer#MAX_VALUE}.
   *
   * This is to match the behavior of DFSInputStream.available(),
   * which some clients may rely on (HBase write-ahead log reading in
   * particular).
   */
  @Override
  public synchronized int available() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    if (pageBlobSize == -1) {
      try {
        pageBlobSize = getPageBlobDataSize(blob, opContext);
      } catch (StorageException e) {
        throw new IOException("Unable to get page blob size.", e);
      }
    }

    final long remaining = pageBlobSize - filePosition;
    return remaining <= Integer.MAX_VALUE ?
        (int) remaining : Integer.MAX_VALUE;
  }

  @Override
  public synchronized void close() throws IOException {
    closed = true;
  }

  private boolean dataAvailableInBuffer() {
    return currentBuffer != null 
        && currentBufferOffset < currentBufferLength;
  }

  /**
   * Check our buffer and download more from the server if needed.
   * If data is not available in the buffer, method downloads maximum
   * page blob download size (4MB) or if there is less then 4MB left,
   * all remaining pages.
   * If we are on the last page, method will return true even if
   * we reached the end of stream.
   * @return true if there's more data in the buffer, false if buffer is empty
   *         and we reached the end of the blob.
   * @throws IOException
   */
  private synchronized boolean ensureDataInBuffer() throws IOException {
    if (dataAvailableInBuffer()) {
      // We still have some data in our buffer.
      return true;
    }
    currentBuffer = null;
    currentBufferOffset = 0;
    currentBufferLength = 0;
    if (numberOfPagesRemaining == 0) {
      // No more data to read.
      return false;
    }
    final long pagesToRead = Math.min(MAX_PAGES_PER_DOWNLOAD,
        numberOfPagesRemaining);
    final int bufferSize = (int) (pagesToRead * PAGE_SIZE);
 
    // Download page to current buffer.
    try {
      // Create a byte array output stream to capture the results of the
      // download.
      ByteArrayOutputStream baos = new ByteArrayOutputStream(bufferSize);
      blob.downloadRange(currentOffsetInBlob, bufferSize, baos,
          withMD5Checking(), opContext);
      validateDataIntegrity(baos.toByteArray());
    } catch (StorageException e) {
      throw new IOException(e);
    }
    numberOfPagesRemaining -= pagesToRead;
    currentOffsetInBlob += bufferSize;

    return true;
  }

  private void validateDataIntegrity(byte[] buffer)
      throws IOException {

    if (buffer.length % PAGE_SIZE != 0) {
      throw new AssertionError("Unexpected buffer size: " 
      + buffer.length);
    }

    int bufferLength = 0;
    int numberOfPages = buffer.length / PAGE_SIZE;
    long totalPagesAfterCurrent = numberOfPagesRemaining;

    for (int page = 0; page < numberOfPages; page++) {
      // Calculate the number of pages that exist in the blob after this one
      totalPagesAfterCurrent--;

      short currentPageSize = getPageSize(blob, buffer, page * PAGE_SIZE);

      // Only the last page can be partially filled.
      if (currentPageSize < PAGE_DATA_SIZE
          && totalPagesAfterCurrent > 0) {
        throw fileCorruptException(blob, String.format(
            "Page with partial data found in the middle (%d pages from the"
             + " end) that only has %d bytes of data.",
             totalPagesAfterCurrent, currentPageSize));
      }
      bufferLength += currentPageSize + PAGE_HEADER_SIZE;
    }

    currentBufferOffset = PAGE_HEADER_SIZE;
    currentBufferLength = bufferLength;
    currentBuffer = buffer;
  }

  // Reads the page size from the page header at the given offset.
  private static short getPageSize(CloudPageBlobWrapper blob,
      byte[] data, int offset) throws IOException {
    short pageSize = toShort(data[offset], data[offset + 1]);
    if (pageSize < 0 || pageSize > PAGE_DATA_SIZE) {
      throw fileCorruptException(blob, String.format(
          "Unexpected page size in the header: %d.",
          pageSize));
    }
    return pageSize;
  }

  @Override
  public synchronized int read(byte[] outputBuffer, int offset, int len)
      throws IOException {
    // If len is zero return 0 per the InputStream contract
    if (len == 0) {
      return 0;
    }

    int numberOfBytesRead = 0;
    while (len > 0) {
      if (!ensureDataInBuffer()) {
        break;
      }
      int bytesRemainingInCurrentPage = getBytesRemainingInCurrentPage();
      int numBytesToRead = Math.min(len, bytesRemainingInCurrentPage);
      System.arraycopy(currentBuffer, currentBufferOffset, outputBuffer,
          offset, numBytesToRead);
      numberOfBytesRead += numBytesToRead;
      offset += numBytesToRead;
      len -= numBytesToRead;
      if (numBytesToRead == bytesRemainingInCurrentPage) {
        // We've finished this page, move on to the next.
        advancePagesInBuffer(1);
      } else {
        currentBufferOffset += numBytesToRead;
      }
    }

    // if outputBuffer len is > 0 and zero bytes were read, we reached
    // an EOF
    if (numberOfBytesRead == 0) {
      return -1;
    }

    filePosition += numberOfBytesRead;
    return numberOfBytesRead;
  }

  @Override
  public int read() throws IOException {
    byte[] oneByte = new byte[1];
    int result = read(oneByte);
    if (result < 0) {
      return result;
    }
    return oneByte[0];
  }

  /**
   * Skips over and discards <code>n</code> bytes of data from this input
   * stream. The <code>skip</code> method may, for a variety of reasons, end
   * up skipping over some smaller number of bytes, possibly <code>0</code>.
   * This may result from any of a number of conditions; reaching end of file
   * before <code>n</code> bytes have been skipped is only one possibility.
   * The actual number of bytes skipped is returned. If {@code n} is
   * negative, the {@code skip} method for class {@code InputStream} always
   * returns 0, and no bytes are skipped. Subclasses may handle the negative
   * value differently.
   *
   * <p> The <code>skip</code> method of this class creates a
   * byte array and then repeatedly reads into it until <code>n</code> bytes
   * have been read or the end of the stream has been reached. Subclasses are
   * encouraged to provide a more efficient implementation of this method.
   * For instance, the implementation may depend on the ability to seek.
   *
   * @param      n   the number of bytes to be skipped.
   * @return     the actual number of bytes skipped.
   * @exception  IOException  if the stream does not support seek,
   *                          or if some other I/O error occurs.
   */
  @Override
  public synchronized long skip(long n) throws IOException {
    long skipped = skipImpl(n);
    filePosition += skipped; // track the position in the stream
    return skipped;
  }

  private long skipImpl(long n) throws IOException {

    if (n == 0) {
      return 0;
    }

    // First skip within the current buffer as much as possible.
    long skippedWithinBuffer = skipWithinBuffer(n);
    if (skippedWithinBuffer > n) {
      // TO CONSIDER: Using a contracts framework such as Google's cofoja for
      // these post-conditions.
      throw new AssertionError(String.format(
          "Bug in skipWithinBuffer: it skipped over %d bytes when asked to "
          + "skip %d bytes.", skippedWithinBuffer, n));
    }
    n -= skippedWithinBuffer;
    long skipped = skippedWithinBuffer;

    if (n == 0) {
      return skipped;
    }

    if (numberOfPagesRemaining == 0) {
      throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
    } else if (numberOfPagesRemaining > 1) {
      // skip over as many pages as we can, but we must read the last
      // page as it may not be full
      long pagesToSkipOver = Math.min(n / PAGE_DATA_SIZE,
          numberOfPagesRemaining - 1);
      numberOfPagesRemaining -= pagesToSkipOver;
      currentOffsetInBlob += pagesToSkipOver * PAGE_SIZE;
      skipped += pagesToSkipOver * PAGE_DATA_SIZE;
      n -= pagesToSkipOver * PAGE_DATA_SIZE;
    }

    if (n == 0) {
      return skipped;
    }

    // Now read in at the current position, and skip within current buffer.
    if (!ensureDataInBuffer()) {
      return skipped;
    }
    return skipped + skipWithinBuffer(n);
  }

  /**
   * Skip over n bytes within the current buffer or just over skip the whole
   * buffer if n is greater than the bytes remaining in the buffer.
   * @param n The number of data bytes to skip.
   * @return The number of bytes actually skipped.
   * @throws IOException if data corruption found in the buffer.
   */
  private long skipWithinBuffer(long n) throws IOException {
    if (!dataAvailableInBuffer()) {
      return 0;
    }
    long skipped = 0;
    // First skip within the current page.
    skipped = skipWithinCurrentPage(n);
    if (skipped > n) {
      throw new AssertionError(String.format(
          "Bug in skipWithinCurrentPage: it skipped over %d bytes when asked" 
          + " to skip %d bytes.", skipped, n));
    }
    n -= skipped;
    if (n == 0 || !dataAvailableInBuffer()) {
      return skipped;
    }

    // Calculate how many whole pages (pages before the possibly partially
    // filled last page) remain.
    int currentPageIndex = currentBufferOffset / PAGE_SIZE;
    int numberOfPagesInBuffer = currentBuffer.length / PAGE_SIZE;
    int wholePagesRemaining = numberOfPagesInBuffer - currentPageIndex - 1;

    if (n < (PAGE_DATA_SIZE * wholePagesRemaining)) {
      // I'm within one of the whole pages remaining, skip in there.
      advancePagesInBuffer((int) (n / PAGE_DATA_SIZE));
      currentBufferOffset += n % PAGE_DATA_SIZE;
      return n + skipped;
    }

    // Skip over the whole pages.
    advancePagesInBuffer(wholePagesRemaining);
    skipped += wholePagesRemaining * PAGE_DATA_SIZE;
    n -= wholePagesRemaining * PAGE_DATA_SIZE;

    // At this point we know we need to skip to somewhere in the last page,
    // or just go to the end.
    return skipWithinCurrentPage(n) + skipped;
  }

  /**
   * Skip over n bytes within the current page or just over skip the whole
   * page if n is greater than the bytes remaining in the page.
   * @param n The number of data bytes to skip.
   * @return The number of bytes actually skipped.
   * @throws IOException if data corruption found in the buffer.
   */
  private long skipWithinCurrentPage(long n) throws IOException {
    int remainingBytesInCurrentPage = getBytesRemainingInCurrentPage();
    if (n <= remainingBytesInCurrentPage) {
      currentBufferOffset += n;
      return n;
    } else {
      advancePagesInBuffer(1);
      return remainingBytesInCurrentPage;
    }
  }

  /**
   * Gets the number of bytes remaining within the current page in the buffer.
   * @return The number of bytes remaining.
   * @throws IOException if data corruption found in the buffer.
   */
  private int getBytesRemainingInCurrentPage() throws IOException {
    if (!dataAvailableInBuffer()) {
      return 0;
    }
    // Calculate our current position relative to the start of the current
    // page.
    int currentDataOffsetInPage =
        (currentBufferOffset % PAGE_SIZE) - PAGE_HEADER_SIZE;
    int pageBoundary = getCurrentPageStartInBuffer();
    // Get the data size of the current page from the header.
    short sizeOfCurrentPage = getPageSize(blob, currentBuffer, pageBoundary);
    return sizeOfCurrentPage - currentDataOffsetInPage;
  }

  private static IOException badStartRangeException(CloudPageBlobWrapper blob,
      PageRange startRange) {
    return fileCorruptException(blob, String.format(
        "Page blobs for ASV should always use a page range starting at byte 0. " 
        + "This starts at byte %d.",
        startRange.getStartOffset()));
  }

  private void advancePagesInBuffer(int numberOfPages) {
    currentBufferOffset =
        getCurrentPageStartInBuffer() 
        + (numberOfPages * PAGE_SIZE) 
        + PAGE_HEADER_SIZE;
  }

  private int getCurrentPageStartInBuffer() {
    return PAGE_SIZE * (currentBufferOffset / PAGE_SIZE);
  }

  private static IOException fileCorruptException(CloudPageBlobWrapper blob,
      String reason) {
    return new IOException(String.format(
        "The page blob: '%s' is corrupt or has an unexpected format: %s.",
        blob.getUri(), reason));
  }
}
