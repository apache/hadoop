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
package org.apache.hadoop.net.unix;

import java.io.Closeable;
import org.apache.hadoop.classification.InterfaceAudience;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.ByteBuffer;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.CloseableReferenceCount;

import com.google.common.annotations.VisibleForTesting;

/**
 * The implementation of UNIX domain sockets in Java.
 * 
 * See {@link DomainSocket} for more information about UNIX domain sockets.
 */
@InterfaceAudience.LimitedPrivate("HDFS")
public class DomainSocket implements Closeable {
  static {
    if (SystemUtils.IS_OS_WINDOWS) {
      loadingFailureReason = "UNIX Domain sockets are not available on Windows.";
    } else if (!NativeCodeLoader.isNativeCodeLoaded()) {
      loadingFailureReason = "libhadoop cannot be loaded.";
    } else {
      String problem;
      try {
        anchorNative();
        problem = null;
      } catch (Throwable t) {
        problem = "DomainSocket#anchorNative got error: " + t.getMessage();
      }
      loadingFailureReason = problem;
    }
  }

  static Log LOG = LogFactory.getLog(DomainSocket.class);

  /**
   * True only if we should validate the paths used in
   * {@link DomainSocket#bindAndListen(String)}
   */
  private static boolean validateBindPaths = true;

  /**
   * The reason why DomainSocket is not available, or null if it is available.
   */
  private final static String loadingFailureReason;

  /**
   * Initialize the native library code.
   */
  private static native void anchorNative();

  /**
   * This function is designed to validate that the path chosen for a UNIX
   * domain socket is secure.  A socket path is secure if it doesn't allow
   * unprivileged users to perform a man-in-the-middle attack against it.
   * For example, one way to perform a man-in-the-middle attack would be for
   * a malicious user to move the server socket out of the way and create his
   * own socket in the same place.  Not good.
   * 
   * Note that we only check the path once.  It's possible that the
   * permissions on the path could change, perhaps to something more relaxed,
   * immediately after the path passes our validation test-- hence creating a
   * security hole.  However, the purpose of this check is to spot common
   * misconfigurations.  System administrators do not commonly change
   * permissions on these paths while the server is running.
   *
   * For more information on Security exceptions see this wiki page:
   * https://wiki.apache.org/hadoop/SocketPathSecurity
   *
   * @param path             the path to validate
   * @param skipComponents   the number of starting path components to skip 
   *                         validation for (used only for testing)
   */
  @VisibleForTesting
  native static void validateSocketPathSecurity0(String path,
      int skipComponents) throws IOException;

  /**
   * Return true only if UNIX domain sockets are available.
   */
  public static String getLoadingFailureReason() {
    return loadingFailureReason;
  }

  /**
   * Disable validation of the server bind paths.
   */
  @VisibleForTesting
  public static void disableBindPathValidation() {
    validateBindPaths = false;
  }

  /**
   * Given a path and a port, compute the effective path by replacing
   * occurrences of _PORT with the port.  This is mainly to make it 
   * possible to run multiple DataNodes locally for testing purposes.
   *
   * @param path            The source path
   * @param port            Port number to use
   *
   * @return                The effective path
   */
  public static String getEffectivePath(String path, int port) {
    return path.replace("_PORT", String.valueOf(port));
  }

  /**
   * The socket reference count and closed bit.
   */
  final CloseableReferenceCount refCount;

  /**
   * The file descriptor associated with this UNIX domain socket.
   */
  final int fd;

  /**
   * The path associated with this UNIX domain socket.
   */
  private final String path;

  /**
   * The InputStream associated with this socket.
   */
  private final DomainInputStream inputStream = new DomainInputStream();

  /**
   * The OutputStream associated with this socket.
   */
  private final DomainOutputStream outputStream = new DomainOutputStream();

  /**
   * The Channel associated with this socket.
   */
  private final DomainChannel channel = new DomainChannel();

  private DomainSocket(String path, int fd) {
    this.refCount = new CloseableReferenceCount();
    this.fd = fd;
    this.path = path;
  }

  private static native int bind0(String path) throws IOException;

  private void unreference(boolean checkClosed) throws ClosedChannelException {
    if (checkClosed) {
      refCount.unreferenceCheckClosed();
    } else {
      refCount.unreference();
    }
  }

  /**
   * Create a new DomainSocket listening on the given path.
   *
   * @param path         The path to bind and listen on.
   * @return             The new DomainSocket.
   */
  public static DomainSocket bindAndListen(String path) throws IOException {
    if (loadingFailureReason != null) {
      throw new UnsupportedOperationException(loadingFailureReason);
    }
    if (validateBindPaths) {
      validateSocketPathSecurity0(path, 0);
    }
    int fd = bind0(path);
    return new DomainSocket(path, fd);
  }

  /**
   * Create a pair of UNIX domain sockets which are connected to each other
   * by calling socketpair(2).
   *
   * @return                An array of two UNIX domain sockets connected to
   *                        each other.
   * @throws IOException    on error.
   */
  public static DomainSocket[] socketpair() throws IOException {
    int fds[] = socketpair0();
    return new DomainSocket[] {
      new DomainSocket("(anonymous0)", fds[0]),
      new DomainSocket("(anonymous1)", fds[1])
    };
  }

  private static native int[] socketpair0() throws IOException;

  private static native int accept0(int fd) throws IOException;

  /**
   * Accept a new UNIX domain connection.
   *
   * This method can only be used on sockets that were bound with bind().
   *
   * @return                The new connection.
   * @throws IOException    If there was an I/O error performing the accept--
   *                        such as the socket being closed from under us.
   *                        Particularly when the accept is timed out, it throws
   *                        SocketTimeoutException.
   */
  public DomainSocket accept() throws IOException {
    refCount.reference();
    boolean exc = true;
    try {
      DomainSocket ret = new DomainSocket(path, accept0(fd));
      exc = false;
      return ret;
    } finally {
      unreference(exc);
    }
  }

  private static native int connect0(String path) throws IOException;

  /**
   * Create a new DomainSocket connected to the given path.
   *
   * @param path              The path to connect to.
   * @throws IOException      If there was an I/O error performing the connect.
   *
   * @return                  The new DomainSocket.
   */
  public static DomainSocket connect(String path) throws IOException {
    if (loadingFailureReason != null) {
      throw new UnsupportedOperationException(loadingFailureReason);
    }
    int fd = connect0(path);
    return new DomainSocket(path, fd);
  }

  /**
   * Return true if the file descriptor is currently open.
   *
   * @return                 True if the file descriptor is currently open.
   */
  public boolean isOpen() {
    return refCount.isOpen();
  }

  /**
   * @return                 The socket path.
   */
  public String getPath() {
    return path;
  }

  /**
   * @return                 The socket InputStream
   */
  public DomainInputStream getInputStream() {
    return inputStream;
  }

  /**
   * @return                 The socket OutputStream
   */
  public DomainOutputStream getOutputStream() {
    return outputStream;
  }

  /**
   * @return                 The socket Channel
   */
  public DomainChannel getChannel() {
    return channel;
  }

  public static final int SEND_BUFFER_SIZE = 1;
  public static final int RECEIVE_BUFFER_SIZE = 2;
  public static final int SEND_TIMEOUT = 3;
  public static final int RECEIVE_TIMEOUT = 4;

  private static native void setAttribute0(int fd, int type, int val)
      throws IOException;

  public void setAttribute(int type, int size) throws IOException {
    refCount.reference();
    boolean exc = true;
    try {
      setAttribute0(fd, type, size);
      exc = false;
    } finally {
      unreference(exc);
    }
  }

  private native int getAttribute0(int fd, int type) throws IOException;

  public int getAttribute(int type) throws IOException {
    refCount.reference();
    int attribute;
    boolean exc = true;
    try {
      attribute = getAttribute0(fd, type);
      exc = false;
      return attribute;
    } finally {
      unreference(exc);
    }
  }

  private static native void close0(int fd) throws IOException;

  private static native void closeFileDescriptor0(FileDescriptor fd)
      throws IOException;

  private static native void shutdown0(int fd) throws IOException;

  /**
   * Close the Socket.
   */
  @Override
  public void close() throws IOException {
    // Set the closed bit on this DomainSocket
    int count;
    try {
      count = refCount.setClosed();
    } catch (ClosedChannelException e) {
      // Someone else already closed the DomainSocket.
      return;
    }
    // Wait for all references to go away
    boolean didShutdown = false;
    boolean interrupted = false;
    while (count > 0) {
      if (!didShutdown) {
        try {
          // Calling shutdown on the socket will interrupt blocking system
          // calls like accept, write, and read that are going on in a
          // different thread.
          shutdown0(fd);
        } catch (IOException e) {
          LOG.error("shutdown error: ", e);
        }
        didShutdown = true;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        interrupted = true;
      }
      count = refCount.getReferenceCount();
    }

    // At this point, nobody has a reference to the file descriptor, 
    // and nobody will be able to get one in the future either.
    // We now call close(2) on the file descriptor.
    // After this point, the file descriptor number will be reused by 
    // something else.  Although this DomainSocket object continues to hold 
    // the old file descriptor number (it's a final field), we never use it 
    // again because this DomainSocket is closed.
    close0(fd);
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
  
  /**
   * Call shutdown(SHUT_RDWR) on the UNIX domain socket.
   *
   * @throws IOException
   */
  public void shutdown() throws IOException {
    refCount.reference();
    boolean exc = true;
    try {
      shutdown0(fd);
      exc = false;
    } finally {
      unreference(exc);
    }
  }

  private native static void sendFileDescriptors0(int fd,
      FileDescriptor descriptors[],
      byte jbuf[], int offset, int length) throws IOException;

  /**
   * Send some FileDescriptor objects to the process on the other side of this
   * socket.
   * 
   * @param descriptors       The file descriptors to send.
   * @param jbuf              Some bytes to send.  You must send at least
   *                          one byte.
   * @param offset            The offset in the jbuf array to start at.
   * @param length            Length of the jbuf array to use.
   */
  public void sendFileDescriptors(FileDescriptor descriptors[],
      byte jbuf[], int offset, int length) throws IOException {
    refCount.reference();
    boolean exc = true;
    try {
      sendFileDescriptors0(fd, descriptors, jbuf, offset, length);
      exc = false;
    } finally {
      unreference(exc);
    }
  }

  private static native int receiveFileDescriptors0(int fd,
      FileDescriptor[] descriptors,
      byte[] buf, int offset, int length) throws IOException;

  /**
   * Receive some FileDescriptor objects from the process on the other side of
   * this socket, and wrap them in FileInputStream objects.
   */
  public int recvFileInputStreams(FileInputStream[] streams, byte buf[],
        int offset, int length) throws IOException {
    FileDescriptor descriptors[] = new FileDescriptor[streams.length];
    boolean success = false;
    for (int i = 0; i < streams.length; i++) {
      streams[i] = null;
    }
    refCount.reference();
    try {
      int ret = receiveFileDescriptors0(fd, descriptors, buf, offset, length);
      for (int i = 0, j = 0; i < descriptors.length; i++) {
        if (descriptors[i] != null) {
          streams[j++] = new FileInputStream(descriptors[i]);
          descriptors[i] = null;
        }
      }
      success = true;
      return ret;
    } finally {
      if (!success) {
        for (int i = 0; i < descriptors.length; i++) {
          if (descriptors[i] != null) {
            try {
              closeFileDescriptor0(descriptors[i]);
            } catch (Throwable t) {
              LOG.warn(t);
            }
          } else if (streams[i] != null) {
            try {
              streams[i].close();
            } catch (Throwable t) {
              LOG.warn(t);
            } finally {
              streams[i] = null; }
          }
        }
      }
      unreference(!success);
    }
  }

  private native static int readArray0(int fd, byte b[], int off, int len)
      throws IOException;
  
  private native static int available0(int fd) throws IOException;

  private static native void write0(int fd, int b) throws IOException;

  private static native void writeArray0(int fd, byte b[], int offset, int length)
      throws IOException;

  private native static int readByteBufferDirect0(int fd, ByteBuffer dst,
      int position, int remaining) throws IOException;

  /**
   * Input stream for UNIX domain sockets.
   */
  @InterfaceAudience.LimitedPrivate("HDFS")
  public class DomainInputStream extends InputStream {
    @Override
    public int read() throws IOException {
      refCount.reference();
      boolean exc = true;
      try {
        byte b[] = new byte[1];
        int ret = DomainSocket.readArray0(DomainSocket.this.fd, b, 0, 1);
        exc = false;
        return (ret >= 0) ? b[0] : -1;
      } finally {
        unreference(exc);
      }
    }
    
    @Override
    public int read(byte b[], int off, int len) throws IOException {
      refCount.reference();
      boolean exc = true;
      try {
        int nRead = DomainSocket.readArray0(DomainSocket.this.fd, b, off, len);
        exc = false;
        return nRead;
      } finally {
        unreference(exc);
      }
    }

    @Override
    public int available() throws IOException {
      refCount.reference();
      boolean exc = true;
      try {
        int nAvailable = DomainSocket.available0(DomainSocket.this.fd);
        exc = false;
        return nAvailable;
      } finally {
        unreference(exc);
      }
    }

    @Override
    public void close() throws IOException {
      DomainSocket.this.close();
    }
  }

  /**
   * Output stream for UNIX domain sockets.
   */
  @InterfaceAudience.LimitedPrivate("HDFS")
  public class DomainOutputStream extends OutputStream {
    @Override
    public void close() throws IOException {
      DomainSocket.this.close();
    }

    @Override
    public void write(int val) throws IOException {
      refCount.reference();
      boolean exc = true;
      try {
        byte b[] = new byte[1];
        b[0] = (byte)val;
        DomainSocket.writeArray0(DomainSocket.this.fd, b, 0, 1);
        exc = false;
      } finally {
        unreference(exc);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      refCount.reference();
      boolean exc = true;
      try {
        DomainSocket.writeArray0(DomainSocket.this.fd, b, off, len);
        exc = false;
      } finally {
        unreference(exc);
      }
    }
  }

  @InterfaceAudience.LimitedPrivate("HDFS")
  public class DomainChannel implements ReadableByteChannel {
    @Override
    public boolean isOpen() {
      return DomainSocket.this.isOpen();
    }

    @Override
    public void close() throws IOException {
      DomainSocket.this.close();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      refCount.reference();
      boolean exc = true;
      try {
        int nread = 0;
        if (dst.isDirect()) {
          nread = DomainSocket.readByteBufferDirect0(DomainSocket.this.fd,
              dst, dst.position(), dst.remaining());
        } else if (dst.hasArray()) {
          nread = DomainSocket.readArray0(DomainSocket.this.fd,
              dst.array(), dst.position() + dst.arrayOffset(),
              dst.remaining());
        } else {
          throw new AssertionError("we don't support " +
              "using ByteBuffers that aren't either direct or backed by " +
              "arrays");
        }
        if (nread > 0) {
          dst.position(dst.position() + nread);
        }
        exc = false;
        return nread;
      } finally {
        unreference(exc);
      }
    }
  }

  @Override
  public String toString() {
    return String.format("DomainSocket(fd=%d,path=%s)", fd, path);
  }
}
