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
import java.lang.reflect.Constructor;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * An utility class for I/O related functionality. 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class IOUtils {
  public static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

  /**
   * Copies from one stream to another.
   *
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param buffSize the size of the buffer 
   * @param close whether or not close the InputStream and 
   * OutputStream at the end. The streams are closed in the finally clause.  
   */
  public static void copyBytes(InputStream in, OutputStream out,
                               int buffSize, boolean close)
    throws IOException {
    try {
      copyBytes(in, out, buffSize);
      if(close) {
        out.close();
        out = null;
        in.close();
        in = null;
      }
    } finally {
      if(close) {
        closeStream(out);
        closeStream(in);
      }
    }
  }
  
  /**
   * Copies from one stream to another.
   * 
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param buffSize the size of the buffer 
   */
  public static void copyBytes(InputStream in, OutputStream out, int buffSize) 
    throws IOException {
    PrintStream ps = out instanceof PrintStream ? (PrintStream)out : null;
    byte buf[] = new byte[buffSize];
    int bytesRead = in.read(buf);
    while (bytesRead >= 0) {
      out.write(buf, 0, bytesRead);
      if ((ps != null) && ps.checkError()) {
        throw new IOException("Unable to write to output stream.");
      }
      bytesRead = in.read(buf);
    }
  }

  /**
   * Copies from one stream to another. <strong>closes the input and output streams 
   * at the end</strong>.
   *
   * @param in InputStrem to read from
   * @param out OutputStream to write to
   * @param conf the Configuration object 
   */
  public static void copyBytes(InputStream in, OutputStream out, Configuration conf)
    throws IOException {
    copyBytes(in, out, conf.getInt(
        IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT), true);
  }
  
  /**
   * Copies from one stream to another.
   *
   * @param in InputStream to read from
   * @param out OutputStream to write to
   * @param conf the Configuration object
   * @param close whether or not close the InputStream and 
   * OutputStream at the end. The streams are closed in the finally clause.
   */
  public static void copyBytes(InputStream in, OutputStream out, Configuration conf, boolean close)
    throws IOException {
    copyBytes(in, out, conf.getInt(
        IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),  close);
  }

  /**
   * Copies count bytes from one stream to another.
   *
   * @param in InputStream to read from
   * @param out OutputStream to write to
   * @param count number of bytes to copy
   * @param close whether to close the streams
   * @throws IOException if bytes can not be read or written
   */
  public static void copyBytes(InputStream in, OutputStream out, long count,
      boolean close) throws IOException {
    byte buf[] = new byte[4096];
    long bytesRemaining = count;
    int bytesRead;

    try {
      while (bytesRemaining > 0) {
        int bytesToRead = (int)
          (bytesRemaining < buf.length ? bytesRemaining : buf.length);

        bytesRead = in.read(buf, 0, bytesToRead);
        if (bytesRead == -1)
          break;

        out.write(buf, 0, bytesRead);
        bytesRemaining -= bytesRead;
      }
      if (close) {
        out.close();
        out = null;
        in.close();
        in = null;
      }
    } finally {
      if (close) {
        closeStream(out);
        closeStream(in);
      }
    }
  }
  
  /**
   * Utility wrapper for reading from {@link InputStream}. It catches any errors
   * thrown by the underlying stream (either IO or decompression-related), and
   * re-throws as an IOException.
   * 
   * @param is - InputStream to be read from
   * @param buf - buffer the data is read into
   * @param off - offset within buf
   * @param len - amount of data to be read
   * @return number of bytes read
   */
  public static int wrappedReadForCompressedData(InputStream is, byte[] buf,
      int off, int len) throws IOException {
    try {
      return is.read(buf, off, len);
    } catch (IOException ie) {
      throw ie;
    } catch (Throwable t) {
      throw new IOException("Error while reading compressed data", t);
    }
  }

  /**
   * Reads len bytes in a loop.
   *
   * @param in InputStream to read from
   * @param buf The buffer to fill
   * @param off offset from the buffer
   * @param len the length of bytes to read
   * @throws IOException if it could not read requested number of bytes 
   * for any reason (including EOF)
   */
  public static void readFully(InputStream in, byte[] buf,
      int off, int len) throws IOException {
    int toRead = len;
    while (toRead > 0) {
      int ret = in.read(buf, off, toRead);
      if (ret < 0) {
        throw new IOException( "Premature EOF from inputStream");
      }
      toRead -= ret;
      off += ret;
    }
  }
  
  /**
   * Similar to readFully(). Skips bytes in a loop.
   * @param in The InputStream to skip bytes from
   * @param len number of bytes to skip.
   * @throws IOException if it could not skip requested number of bytes 
   * for any reason (including EOF)
   */
  public static void skipFully(InputStream in, long len) throws IOException {
    long amt = len;
    while (amt > 0) {
      long ret = in.skip(amt);
      if (ret == 0) {
        // skip may return 0 even if we're not at EOF.  Luckily, we can 
        // use the read() method to figure out if we're at the end.
        int b = in.read();
        if (b == -1) {
          throw new EOFException( "Premature EOF from inputStream after " +
              "skipping " + (len - amt) + " byte(s).");
        }
        ret = 1;
      }
      amt -= ret;
    }
  }
  
  /**
   * Close the Closeable objects and <b>ignore</b> any {@link Throwable} or
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param log the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   * @deprecated use {@link #cleanupWithLogger(Logger, java.io.Closeable...)}
   * instead
   */
  @Deprecated
  public static void cleanup(Log log, java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch(Throwable e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }

  /**
   * Close the Closeable objects and <b>ignore</b> any {@link Throwable} or
   * null pointers. Must only be used for cleanup in exception handlers.
   *
   * @param logger the log to record problems to at debug level. Can be null.
   * @param closeables the objects to close
   */
  public static void cleanupWithLogger(Logger logger,
      java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch (Throwable e) {
          if (logger != null) {
            logger.debug("Exception in closing {}", c, e);
          }
        }
      }
    }
  }

  /**
   * Closes the stream ignoring {@link Throwable}.
   * Must only be called in cleaning up from exception handlers.
   *
   * @param stream the Stream to close
   */
  public static void closeStream(java.io.Closeable stream) {
    if (stream != null) {
      cleanupWithLogger(null, stream);
    }
  }

  /**
   * Closes the streams ignoring {@link Throwable}.
   * Must only be called in cleaning up from exception handlers.
   *
   * @param streams the Streams to close
   */
  public static void closeStreams(java.io.Closeable... streams) {
    if (streams != null) {
      cleanupWithLogger(null, streams);
    }
  }

  /**
   * Closes the socket ignoring {@link IOException}
   *
   * @param sock the Socket to close
   */
  public static void closeSocket(Socket sock) {
    if (sock != null) {
      try {
        sock.close();
      } catch (IOException ignored) {
        LOG.debug("Ignoring exception while closing socket", ignored);
      }
    }
  }
  
  /**
   * The /dev/null of OutputStreams.
   */
  public static class NullOutputStream extends OutputStream {
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
    }

    @Override
    public void write(int b) throws IOException {
    }
  }  
  
  /**
   * Write a ByteBuffer to a WritableByteChannel, handling short writes.
   * 
   * @param bc               The WritableByteChannel to write to
   * @param buf              The input buffer
   * @throws IOException     On I/O error
   */
  public static void writeFully(WritableByteChannel bc, ByteBuffer buf)
      throws IOException {
    do {
      bc.write(buf);
    } while (buf.remaining() > 0);
  }

  /**
   * Write a ByteBuffer to a FileChannel at a given offset, 
   * handling short writes.
   * 
   * @param fc               The FileChannel to write to
   * @param buf              The input buffer
   * @param offset           The offset in the file to start writing at
   * @throws IOException     On I/O error
   */
  public static void writeFully(FileChannel fc, ByteBuffer buf,
      long offset) throws IOException {
    do {
      offset += fc.write(buf, offset);
    } while (buf.remaining() > 0);
  }

  /**
   * Return the complete list of files in a directory as strings.<p/>
   *
   * This is better than File#listDir because it does not ignore IOExceptions.
   *
   * @param dir              The directory to list.
   * @param filter           If non-null, the filter to use when listing
   *                         this directory.
   * @return                 The list of files in the directory.
   *
   * @throws IOException     On I/O error
   */
  public static List<String> listDirectory(File dir, FilenameFilter filter)
      throws IOException {
    ArrayList<String> list = new ArrayList<String> ();
    try (DirectoryStream<Path> stream =
             Files.newDirectoryStream(dir.toPath())) {
      for (Path entry: stream) {
        Path fileName = entry.getFileName();
        if (fileName != null) {
          String fileNameStr = fileName.toString();
          if ((filter == null) || filter.accept(dir, fileNameStr)) {
            list.add(fileNameStr);
          }
        }
      }
    } catch (DirectoryIteratorException e) {
      throw e.getCause();
    }
    return list;
  }

  /**
   * Ensure that any writes to the given file is written to the storage device
   * that contains it. This method opens channel on given File and closes it
   * once the sync is done.<br>
   * Borrowed from Uwe Schindler in LUCENE-5588
   * @param fileToSync the file to fsync
   */
  public static void fsync(File fileToSync) throws IOException {
    if (!fileToSync.exists()) {
      throw new FileNotFoundException(
          "File/Directory " + fileToSync.getAbsolutePath() + " does not exist");
    }
    boolean isDir = fileToSync.isDirectory();

    // HDFS-13586, FileChannel.open fails with AccessDeniedException
    // for any directory, ignore.
    if (isDir && Shell.WINDOWS) {
      return;
    }

    // If the file is a directory we have to open read-only, for regular files
    // we must open r/w for the fsync to have an effect. See
    // http://blog.httrack.com/blog/2013/11/15/
    // everything-you-always-wanted-to-know-about-fsync/
    try(FileChannel channel = FileChannel.open(fileToSync.toPath(),
        isDir ? StandardOpenOption.READ : StandardOpenOption.WRITE)){
      fsync(channel, isDir);
    }
  }

  /**
   * Ensure that any writes to the given file is written to the storage device
   * that contains it. This method opens channel on given File and closes it
   * once the sync is done.
   * Borrowed from Uwe Schindler in LUCENE-5588
   * @param channel Channel to sync
   * @param isDir if true, the given file is a directory (Channel should be
   *          opened for read and ignore IOExceptions, because not all file
   *          systems and operating systems allow to fsync on a directory)
   * @throws IOException
   */
  public static void fsync(FileChannel channel, boolean isDir)
      throws IOException {
    try {
      channel.force(true);
    } catch (IOException ioe) {
      if (isDir) {
        assert !(Shell.LINUX
            || Shell.MAC) : "On Linux and MacOSX fsyncing a directory"
                + " should not throw IOException, we just don't want to rely"
                + " on that in production (undocumented)" + ". Got: " + ioe;
        // Ignore exception if it is a directory
        return;
      }
      // Throw original exception
      throw ioe;
    }
  }

  /**
   * Takes an IOException, file/directory path, and method name and returns an
   * IOException with the input exception as the cause and also include the
   * file,method details. The new exception provides the stack trace of the
   * place where the exception is thrown and some extra diagnostics
   * information.
   *
   * Return instance of same exception if exception class has a public string
   * constructor; Otherwise return an PathIOException.
   * InterruptedIOException and PathIOException are returned unwrapped.
   *
   * @param path file/directory path
   * @param methodName method name
   * @param exception the caught exception.
   * @return an exception to throw
   */
  public static IOException wrapException(final String path,
      final String methodName, final IOException exception) {

    if (exception instanceof InterruptedIOException
        || exception instanceof PathIOException) {
      return exception;
    } else {
      String msg = String
          .format("Failed with %s while processing file/directory :[%s] in "
                  + "method:[%s]",
              exception.getClass().getName(), path, methodName);
      try {
        return wrapWithMessage(exception, msg);
      } catch (Exception ex) {
        // For subclasses which have no (String) constructor throw IOException
        // with wrapped message

        return new PathIOException(path, exception);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends IOException> T wrapWithMessage(
      final T exception, final String msg) throws T {
    Class<? extends Throwable> clazz = exception.getClass();
    try {
      Constructor<? extends Throwable> ctor = clazz
          .getConstructor(String.class);
      Throwable t = ctor.newInstance(msg);
      return (T) (t.initCause(exception));
    } catch (Throwable e) {
      LOG.warn("Unable to wrap exception of type " +
          clazz + ": it has no (String) constructor", e);
      throw exception;
    }
  }

  /**
   * Reads a DataInput until EOF and returns a byte array.  Make sure not to
   * pass in an infinite DataInput or this will never return.
   *
   * @param in A DataInput
   * @return a byte array containing the data from the DataInput
   * @throws IOException on I/O error, other than EOF
   */
  public static byte[] readFullyToByteArray(DataInput in) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      while (true) {
        baos.write(in.readByte());
      }
    } catch (EOFException eof) {
      // finished reading, do nothing
    }
    return baos.toByteArray();
  }
}
