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

package org.apache.hadoop.io.wrappedio;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BulkDelete;
import org.apache.hadoop.fs.ByteBufferPositionedReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathCapabilities;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.util.functional.FutureIO;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.util.functional.FunctionalIO.uncheckIOExceptions;

/**
 * Reflection-friendly access to APIs which are not available in
 * some of the older Hadoop versions which libraries still
 * compile against.
 * <p>
 * The intent is to avoid the need for complex reflection operations
 * including wrapping of parameter classes, direct instantiation of
 * new classes etc.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class WrappedIO {

  private WrappedIO() {
  }

  /**
   * Get the maximum number of objects/files to delete in a single request.
   * @param fs filesystem
   * @param path path to delete under.
   * @return a number greater than or equal to zero.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException path not valid.
   * @throws UncheckedIOException if an IOE was raised.
   */
  public static int bulkDelete_pageSize(FileSystem fs, Path path) {

    return uncheckIOExceptions(() -> {
      try (BulkDelete bulk = fs.createBulkDelete(path)) {
        return bulk.pageSize();
      }
    });
  }

  /**
   * Delete a list of files/objects.
   * <ul>
   *   <li>Files must be under the path provided in {@code base}.</li>
   *   <li>The size of the list must be equal to or less than the page size.</li>
   *   <li>Directories are not supported; the outcome of attempting to delete
   *       directories is undefined (ignored; undetected, listed as failures...).</li>
   *   <li>The operation is not atomic.</li>
   *   <li>The operation is treated as idempotent: network failures may
   *        trigger resubmission of the request -any new objects created under a
   *        path in the list may then be deleted.</li>
   *    <li>There is no guarantee that any parent directories exist after this call.
   *    </li>
   * </ul>
   * @param fs filesystem
   * @param base path to delete under.
   * @param paths list of paths which must be absolute and under the base path.
   * @return a list of all the paths which couldn't be deleted for a reason other
   *          than "not found" and any associated error message.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws UncheckedIOException if an IOE was raised.
   * @throws IllegalArgumentException if a path argument is invalid.
   */
  public static List<Map.Entry<Path, String>> bulkDelete_delete(FileSystem fs,
      Path base,
      Collection<Path> paths) {

    return uncheckIOExceptions(() -> {
      try (BulkDelete bulk = fs.createBulkDelete(base)) {
        return bulk.bulkDelete(paths);
      }
    });
  }

  /**
   * Does a path have a given capability?
   * Calls {@link PathCapabilities#hasPathCapability(Path, String)},
   * mapping IOExceptions to false.
   * @param fs filesystem
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   * @return true if the capability is supported under that part of the FS.
   * resolving paths or relaying the call.
   * @throws IllegalArgumentException invalid arguments
   */
  public static boolean pathCapabilities_hasPathCapability(Object fs,
      Path path,
      String capability) {
    try {
      return ((PathCapabilities) fs).hasPathCapability(path, capability);
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Does an object implement {@link StreamCapabilities} and, if so,
   * what is the result of the probe for the capability?
   * Calls {@link StreamCapabilities#hasCapability(String)},
   * @param object object to probe
   * @param capability capability string
   * @return true iff the object implements StreamCapabilities and the capability is
   * declared available.
   */
  public static boolean streamCapabilities_hasCapability(Object object, String capability) {
    if (!(object instanceof StreamCapabilities)) {
      return false;
    }
    return ((StreamCapabilities) object).hasCapability(capability);
  }

  /**
   * OpenFile assistant, easy reflection-based access to
   * {@link FileSystem#openFile(Path)} and blocks
   * awaiting the operation completion.
   * @param fs filesystem
   * @param path path
   * @param policy read policy
   * @param status optional file status
   * @param length optional file length
   * @param options nullable map of other options
   * @return stream of the opened file
   * @throws UncheckedIOException if an IOE was raised.
   */
  @InterfaceStability.Stable
  public static FSDataInputStream fileSystem_openFile(
      final FileSystem fs,
      final Path path,
      final String policy,
      @Nullable final FileStatus status,
      @Nullable final Long length,
      @Nullable final Map<String, String> options) {
    final FutureDataInputStreamBuilder builder = uncheckIOExceptions(() ->
        fs.openFile(path));
    if (policy != null) {
      builder.opt(FS_OPTION_OPENFILE_READ_POLICY, policy);
    }
    if (status != null) {
      builder.withFileStatus(status);
    }
    if (length != null) {
      builder.opt(FS_OPTION_OPENFILE_LENGTH, Long.toString(length));
    }
    if (options != null) {
      // add all the options map entries
      options.forEach(builder::opt);
    }
    // wait for the opening.
    return uncheckIOExceptions(() ->
        FutureIO.awaitFuture(builder.build()));
  }

  /**
   * Return path of the enclosing root for a given path.
   * The enclosing root path is a common ancestor that should be used for temp and staging dirs
   * as well as within encryption zones and other restricted directories.
   * @param fs filesystem
   * @param path file path to find the enclosing root path for
   * @return a path to the enclosing root
   * @throws IOException early checks like failure to resolve path cause IO failures
   */
  public static Path fileSystem_getEnclosingRoot(FileSystem fs, Path path) throws IOException {
    return fs.getEnclosingRoot(path);
  }

  /**
   * Delegate to {@link ByteBufferPositionedReadable#read(long, ByteBuffer)}.
   * @param in input stream
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * Note: that is the default behaviour of {@link FSDataInputStream#readFully(long, ByteBuffer)}.
   */
  public static void byteBufferPositionedReadable_readFully(
      InputStream in,
      long position,
      ByteBuffer buf) {
    if (!(in instanceof ByteBufferPositionedReadable)) {
      throw new UnsupportedOperationException("Not a ByteBufferPositionedReadable: " + in);
    }
    uncheckIOExceptions(() -> {
      ((ByteBufferPositionedReadable) in).readFully(position, buf);
      return null;
    });
  }

  /**
   * Probe to see if the input stream is an instance of ByteBufferPositionedReadable.
   * If the stream is an FSDataInputStream, the wrapped stream is checked.
   * @param in input stream
   * @return true if the stream implements the interface (including a wrapped stream)
   * and that it declares the stream capability.
   */
  public static boolean byteBufferPositionedReadable_readFullyAvailable(
      InputStream in) {
    if (!(in instanceof ByteBufferPositionedReadable)) {
      return false;
    }
    if (in instanceof FSDataInputStream) {
      // ask the wrapped stream.
      return byteBufferPositionedReadable_readFullyAvailable(
          ((FSDataInputStream) in).getWrappedStream());
    }
    // now rely on the input stream implementing path capabilities, which
    // all the Hadoop FS implementations do.
    return streamCapabilities_hasCapability(in, StreamCapabilities.PREADBYTEBUFFER);
  }
}
