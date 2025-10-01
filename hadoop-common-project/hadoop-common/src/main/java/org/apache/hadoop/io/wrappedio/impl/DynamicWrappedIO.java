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

package org.apache.hadoop.io.wrappedio.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.dynamic.DynMethods;

import static org.apache.hadoop.util.dynamic.BindingUtils.available;
import static org.apache.hadoop.util.dynamic.BindingUtils.checkAvailable;
import static org.apache.hadoop.util.dynamic.BindingUtils.extractIOEs;
import static org.apache.hadoop.util.dynamic.BindingUtils.loadClass;
import static org.apache.hadoop.util.dynamic.BindingUtils.loadStaticMethod;

/**
 * The wrapped IO methods in {@code org.apache.hadoop.io.wrappedio.WrappedIO},
 * dynamically loaded.
 */
public final class DynamicWrappedIO {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicWrappedIO.class);

  /**
   * Classname of the wrapped IO class: {@value}.
   */
  private static final String WRAPPED_IO_CLASSNAME =
      "org.apache.hadoop.io.wrappedio.WrappedIO";

  /**
   * Method name for openFile: {@value}.
   */
  private static final String FILESYSTEM_OPEN_FILE = "fileSystem_openFile";

  /**
   * Method name for bulk delete: {@value}.
   */
  private static final String BULKDELETE_DELETE = "bulkDelete_delete";

  /**
   * Method name for bulk delete: {@value}.
   */
  private static final String BULKDELETE_PAGESIZE = "bulkDelete_pageSize";

  /**
   * Method name for {@code byteBufferPositionedReadable}: {@value}.
   */
  private static final String BYTE_BUFFER_POSITIONED_READABLE_READ_FULLY_AVAILABLE =
      "byteBufferPositionedReadable_readFullyAvailable";

  /**
   * Method name for {@code byteBufferPositionedReadable}: {@value}.
   */
  private static final String BYTE_BUFFER_POSITIONED_READABLE_READ_FULLY =
      "byteBufferPositionedReadable_readFully";

  /**
   * Method name for {@code PathCapabilities.hasPathCapability()}.
   * {@value}
   */
  private static final String PATH_CAPABILITIES_HAS_PATH_CAPABILITY =
      "pathCapabilities_hasPathCapability";

  /**
   * Method name for {@code StreamCapabilities.hasCapability()}.
   * {@value}
   */
  private static final String STREAM_CAPABILITIES_HAS_CAPABILITY =
      "streamCapabilities_hasCapability";

  /**
   * A singleton instance of the wrapper.
   */
  private static final DynamicWrappedIO INSTANCE = new DynamicWrappedIO();

  /**
   * Read policy for parquet files: {@value}.
   */
  public static final String PARQUET_READ_POLICIES = "parquet, columnar, vector, random";

  /**
   * Was wrapped IO loaded?
   * In the hadoop codebase, this is true.
   * But in other libraries it may not always be true...this
   * field is used to assist copy-and-paste adoption.
   */
  private final boolean loaded;

  /**
   * Method binding.
   * {@code WrappedIO.bulkDelete_delete(FileSystem, Path, Collection)}.
   */
  private final DynMethods.UnboundMethod bulkDeleteDeleteMethod;

  /**
   * Method binding.
   * {@code WrappedIO.bulkDelete_pageSize(FileSystem, Path)}.
   */
  private final DynMethods.UnboundMethod bulkDeletePageSizeMethod;

  /**
   * Dynamic openFile() method.
   * {@code WrappedIO.fileSystem_openFile(FileSystem, Path, String, FileStatus, Long, Map)}.
   */
  private final DynMethods.UnboundMethod fileSystemOpenFileMethod;

  private final DynMethods.UnboundMethod pathCapabilitiesHasPathCapabilityMethod;

  private final DynMethods.UnboundMethod streamCapabilitiesHasCapabilityMethod;

  private final DynMethods.UnboundMethod byteBufferPositionedReadableReadFullyAvailableMethod;

  private final DynMethods.UnboundMethod byteBufferPositionedReadableReadFullyMethod;

  public DynamicWrappedIO() {
    this(WRAPPED_IO_CLASSNAME);
  }

  public DynamicWrappedIO(String classname) {

    // Wrapped IO class.
    Class<?> wrappedClass = loadClass(classname);

    loaded = wrappedClass != null;

    // bulk delete APIs
    bulkDeleteDeleteMethod = loadStaticMethod(
        wrappedClass,
        List.class,
        BULKDELETE_DELETE,
        FileSystem.class,
        Path.class,
        Collection.class);

    bulkDeletePageSizeMethod = loadStaticMethod(
        wrappedClass,
        Integer.class,
        BULKDELETE_PAGESIZE,
        FileSystem.class,
        Path.class);

    // load the openFile method
    fileSystemOpenFileMethod = loadStaticMethod(
        wrappedClass,
        FSDataInputStream.class,
        FILESYSTEM_OPEN_FILE,
        FileSystem.class,
        Path.class,
        String.class,
        FileStatus.class,
        Long.class,
        Map.class);

    // path and stream capabilities
    pathCapabilitiesHasPathCapabilityMethod = loadStaticMethod(wrappedClass,
        boolean.class,
        PATH_CAPABILITIES_HAS_PATH_CAPABILITY,
        Object.class,
        Path.class,
        String.class);

    streamCapabilitiesHasCapabilityMethod = loadStaticMethod(wrappedClass,
        boolean.class,
        STREAM_CAPABILITIES_HAS_CAPABILITY,
        Object.class,
        String.class);

    // ByteBufferPositionedReadable
    byteBufferPositionedReadableReadFullyAvailableMethod = loadStaticMethod(wrappedClass,
        Void.class,
        BYTE_BUFFER_POSITIONED_READABLE_READ_FULLY_AVAILABLE,
        InputStream.class);

    byteBufferPositionedReadableReadFullyMethod = loadStaticMethod(wrappedClass,
        Void.class,
        BYTE_BUFFER_POSITIONED_READABLE_READ_FULLY,
        InputStream.class,
        long.class,
        ByteBuffer.class);

  }

  /**
   * Is the wrapped IO class loaded?
   * @return true if the wrappedIO class was found and loaded.
   */
  public boolean loaded() {
    return loaded;
  }


  /**
   * For testing: verify that all methods were found.
   * @throws UnsupportedOperationException if the method was not found.
   */
  void requireAllMethodsAvailable()  throws UnsupportedOperationException {

    final DynMethods.UnboundMethod[] methods = {
        bulkDeleteDeleteMethod,
        bulkDeletePageSizeMethod,
        fileSystemOpenFileMethod,
        pathCapabilitiesHasPathCapabilityMethod,
        streamCapabilitiesHasCapabilityMethod,
        byteBufferPositionedReadableReadFullyAvailableMethod,
        byteBufferPositionedReadableReadFullyMethod,
    };
    for (DynMethods.UnboundMethod method : methods) {
      LOG.info("Checking method {}", method);
      if (!available(method)) {
        throw new UnsupportedOperationException("Unbound " + method);
      }
    }
  }


  /**
   * Are the bulk delete methods available?
   * @return true if the methods were found.
   */
  public boolean bulkDelete_available() {
    return available(bulkDeleteDeleteMethod);
  }

  /**
   * Get the maximum number of objects/files to delete in a single request.
   * @param fileSystem filesystem
   * @param path path to delete under.
   * @return a number greater than or equal to zero.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException path not valid.
   * @throws IOException problems resolving paths
   * @throws RuntimeException invocation failure.
   */
  public int bulkDelete_pageSize(final FileSystem fileSystem, final Path path)
      throws IOException {
    checkAvailable(bulkDeletePageSizeMethod);
    return extractIOEs(() ->
        bulkDeletePageSizeMethod.invoke(null, fileSystem, path));
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
   * @return a list of all the paths which couldn't be deleted for a reason other than
   *          "not found" and any associated error message.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException if a path argument is invalid.
   * @throws IOException IO problems including networking, authentication and more.
   */
  public List<Map.Entry<Path, String>> bulkDelete_delete(FileSystem fs,
      Path base,
      Collection<Path> paths) throws IOException {
    checkAvailable(bulkDeleteDeleteMethod);
    return extractIOEs(() ->
        bulkDeleteDeleteMethod.invoke(null, fs, base, paths));
  }

  /**
   * Is the {@link #fileSystem_openFile(FileSystem, Path, String, FileStatus, Long, Map)}
   * method available.
   * @return true if the optimized open file method can be invoked.
   */
  public boolean fileSystem_openFile_available() {
    return available(fileSystemOpenFileMethod);
  }

  /**
   * OpenFile assistant, easy reflection-based access to
   * {@code FileSystem#openFile(Path)} and blocks
   * awaiting the operation completion.
   * @param fs filesystem
   * @param path path
   * @param policy read policy
   * @param status optional file status
   * @param length optional file length
   * @param options nullable map of other options
   * @return stream of the opened file
   * @throws IOException if the operation was attempted and failed.
   */
  public FSDataInputStream fileSystem_openFile(
      final FileSystem fs,
      final Path path,
      final String policy,
      @Nullable final FileStatus status,
      @Nullable final Long length,
      @Nullable final Map<String, String> options)
      throws IOException {
    checkAvailable(fileSystemOpenFileMethod);
    return extractIOEs(() ->
        fileSystemOpenFileMethod.invoke(null,
            fs, path, policy, status, length, options));
  }

  /**
   * Does a path have a given capability?
   * Calls {@code PathCapabilities#hasPathCapability(Path, String)},
   * mapping IOExceptions to false.
   * @param fs filesystem
   * @param path path to query the capability of.
   * @param capability non-null, non-empty string to query the path for support.
   * @return true if the capability is supported
   * under that part of the FS
   * false if the method is not loaded or the path lacks the capability.
   * @throws IllegalArgumentException invalid arguments
   */
  public boolean pathCapabilities_hasPathCapability(Object fs,
      Path path,
      String capability) {
    if (!available(pathCapabilitiesHasPathCapabilityMethod)) {
      return false;
    }
    return pathCapabilitiesHasPathCapabilityMethod.invoke(null, fs, path, capability);
  }

  /**
   * Does an object implement {@code StreamCapabilities} and, if so,
   * what is the result of the probe for the capability?
   * Calls {@code StreamCapabilities#hasCapability(String)},
   * @param object object to probe
   * @param capability capability string
   * @return true iff the object implements StreamCapabilities and the capability is
   * declared available.
   */
  public boolean streamCapabilities_hasCapability(Object object, String capability) {
    if (!available(streamCapabilitiesHasCapabilityMethod)) {
      return false;
    }
    return streamCapabilitiesHasCapabilityMethod.invoke(null, object, capability);
  }

  /**
   * Are the ByteBufferPositionedReadable methods loaded?
   * This does not check that a specific stream implements the API;
   * use {@link #byteBufferPositionedReadable_readFullyAvailable(InputStream)}.
   * @return true if the hadoop libraries have the method.
   */
  public boolean byteBufferPositionedReadable_available() {
    return available(byteBufferPositionedReadableReadFullyAvailableMethod);
  }

  /**
   * Probe to see if the input stream is an instance of ByteBufferPositionedReadable.
   * If the stream is an FSDataInputStream, the wrapped stream is checked.
   * @param in input stream
   * @return true if the API is available, the stream implements the interface
   * (including the innermost wrapped stream) and that it declares the stream capability.
   * @throws IOException if the operation was attempted and failed.
   */
  public boolean byteBufferPositionedReadable_readFullyAvailable(
      InputStream in) throws IOException {
    if (available(byteBufferPositionedReadableReadFullyAvailableMethod)) {
      return extractIOEs(() ->
          byteBufferPositionedReadableReadFullyAvailableMethod.invoke(null, in));
    } else {
      return false;
    }
  }

  /**
   * Delegate to {@code ByteBufferPositionedReadable#read(long, ByteBuffer)}.
   * @param in input stream
   * @param position position within file
   * @param buf the ByteBuffer to receive the results of the read operation.
   * @throws UnsupportedOperationException if the input doesn't implement
   * the interface or, if when invoked, it is raised.
   * Note: that is the default behaviour of {@code FSDataInputStream#readFully(long, ByteBuffer)}.
   * @throws IOException if the operation was attempted and failed.
   */
  public void byteBufferPositionedReadable_readFully(
      InputStream in,
      long position,
      ByteBuffer buf) throws IOException {
    checkAvailable(byteBufferPositionedReadableReadFullyMethod);
    extractIOEs(() ->
        byteBufferPositionedReadableReadFullyMethod.invoke(null, in, position, buf));
  }

  /**
   * Get the singleton instance.
   * @return the instance
   */
  public static DynamicWrappedIO instance() {
    return INSTANCE;
  }

  /**
   * Is the wrapped IO class loaded?
   * @return true if the instance is loaded.
   */
  public static boolean isAvailable() {
    return instance().loaded();
  }

  /**
   * Open a file.
   * <p>
   * If the WrappedIO class is found, use it.
   * <p>
   * If not, falls back to the classic {@code fs.open(Path)} call.
   * @param fs filesystem
   * @param status file status
   * @param readPolicies read policy to use
   * @return the input stream
   * @throws IOException any IO failure.
   */
  public static FSDataInputStream openFile(
      FileSystem fs,
      FileStatus status,
      String readPolicies) throws IOException {
    return openFileOnInstance(instance(), fs, status, readPolicies);
  }

  /**
   * Open a file.
   * <p>
   * If the WrappedIO class is found, uses
   * {@link #fileSystem_openFile(FileSystem, Path, String, FileStatus, Long, Map)} with
   * {@link #PARQUET_READ_POLICIES} as the list of read policies and passing down
   * the file status.
   * <p>
   * If not, falls back to the classic {@code fs.open(Path)} call.
   * @param instance dynamic wrapped IO instance.
   * @param fs filesystem
   * @param status file status
   * @param readPolicies read policy to use
   * @return the input stream
   * @throws IOException any IO failure.
   */
  @VisibleForTesting
  static FSDataInputStream openFileOnInstance(
      DynamicWrappedIO instance,
      FileSystem fs,
      FileStatus status,
      String readPolicies) throws IOException {
    FSDataInputStream stream;
    if (instance.fileSystem_openFile_available()) {
      // use openfile for a higher performance read
      // and the ability to set a read policy.
      // This optimizes for cloud storage by saving on IO
      // in open and choosing the range for GET requests.
      // For other stores, it ultimately invokes the classic open(Path)
      // call so is no more expensive than before.
      LOG.debug("Opening file {} through fileSystem_openFile", status);
      stream = instance.fileSystem_openFile(fs,
          status.getPath(),
          readPolicies,
          status,
          null,
          null);
    } else {
      LOG.debug("Opening file {} through open()", status);
      stream = fs.open(status.getPath());
    }
    return stream;
  }

}
