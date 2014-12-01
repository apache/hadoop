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
import java.io.EOFException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * The DomainSocketWatcher watches a set of domain sockets to see when they
 * become readable, or closed.  When one of those events happens, it makes a
 * callback.
 *
 * See {@link DomainSocket} for more information about UNIX domain sockets.
 */
@InterfaceAudience.LimitedPrivate("HDFS")
public final class DomainSocketWatcher implements Closeable {
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
        problem = "DomainSocketWatcher#anchorNative got error: " +
          t.getMessage();
      }
      loadingFailureReason = problem;
    }
  }

  static Log LOG = LogFactory.getLog(DomainSocketWatcher.class);

  /**
   * The reason why DomainSocketWatcher is not available, or null if it is
   * available.
   */
  private final static String loadingFailureReason;

  /**
   * Initializes the native library code.
   */
  private static native void anchorNative();

  public static String getLoadingFailureReason() {
    return loadingFailureReason;
  }

  public interface Handler {
    /**
     * Handles an event on a socket.  An event may be the socket becoming
     * readable, or the remote end being closed.
     *
     * @param sock    The socket that the event occurred on.
     * @return        Whether we should close the socket.
     */
    boolean handle(DomainSocket sock);
  }

  /**
   * Handler for {DomainSocketWatcher#notificationSockets[1]}
   */
  private class NotificationHandler implements Handler {
    public boolean handle(DomainSocket sock) {
      assert(lock.isHeldByCurrentThread());
      try {
        kicked = false;
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + ": NotificationHandler: doing a read on " +
            sock.fd);
        }
        if (sock.getInputStream().read() == -1) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(this + ": NotificationHandler: got EOF on " + sock.fd);
          }
          throw new EOFException();
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + ": NotificationHandler: read succeeded on " +
            sock.fd);
        }
        return false;
      } catch (IOException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + ": NotificationHandler: setting closed to " +
              "true for " + sock.fd);
        }
        closed = true;
        return true;
      }
    }
  }

  private static class Entry {
    final DomainSocket socket;
    final Handler handler;

    Entry(DomainSocket socket, Handler handler) {
      this.socket = socket;
      this.handler = handler;
    }

    DomainSocket getDomainSocket() {
      return socket;
    }

    Handler getHandler() {
      return handler;
    }
  }

  /**
   * The FdSet is a set of file descriptors that gets passed to poll(2).
   * It contains a native memory segment, so that we don't have to copy
   * in the poll0 function.
   */
  private static class FdSet {
    private long data;

    private native static long alloc0();

    FdSet() {
      data = alloc0();
    }

    /**
     * Add a file descriptor to the set.
     *
     * @param fd   The file descriptor to add.
     */
    native void add(int fd);

    /**
     * Remove a file descriptor from the set.
     *
     * @param fd   The file descriptor to remove.
     */
    native void remove(int fd);

    /**
     * Get an array containing all the FDs marked as readable.
     * Also clear the state of all FDs.
     *
     * @return     An array containing all of the currently readable file
     *             descriptors.
     */
    native int[] getAndClearReadableFds();

    /**
     * Close the object and de-allocate the memory used.
     */
    native void close();
  }

  /**
   * Lock which protects toAdd, toRemove, and closed.
   */
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * Condition variable which indicates that toAdd and toRemove have been
   * processed.
   */
  private final Condition processedCond = lock.newCondition();

  /**
   * Entries to add.
   */
  private final LinkedList<Entry> toAdd =
      new LinkedList<Entry>();

  /**
   * Entries to remove.
   */
  private final TreeMap<Integer, DomainSocket> toRemove =
      new TreeMap<Integer, DomainSocket>();

  /**
   * Maximum length of time to go between checking whether the interrupted
   * bit has been set for this thread.
   */
  private final int interruptCheckPeriodMs;

  /**
   * A pair of sockets used to wake up the thread after it has called poll(2).
   */
  private final DomainSocket notificationSockets[];

  /**
   * Whether or not this DomainSocketWatcher is closed.
   */
  private boolean closed = false;
  
  /**
   * True if we have written a byte to the notification socket. We should not
   * write anything else to the socket until the notification handler has had a
   * chance to run. Otherwise, our thread might block, causing deadlock. 
   * See HADOOP-11333 for details.
   */
  private boolean kicked = false;

  public DomainSocketWatcher(int interruptCheckPeriodMs) throws IOException {
    if (loadingFailureReason != null) {
      throw new UnsupportedOperationException(loadingFailureReason);
    }
    Preconditions.checkArgument(interruptCheckPeriodMs > 0);
    this.interruptCheckPeriodMs = interruptCheckPeriodMs;
    notificationSockets = DomainSocket.socketpair();
    watcherThread.setDaemon(true);
    watcherThread.start();
  }

  /**
   * Close the DomainSocketWatcher and wait for its thread to terminate.
   *
   * If there is more than one close, all but the first will be ignored.
   */
  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      if (closed) return;
      if (LOG.isDebugEnabled()) {
        LOG.debug(this + ": closing");
      }
      closed = true;
    } finally {
      lock.unlock();
    }
    // Close notificationSockets[0], so that notificationSockets[1] gets an EOF
    // event.  This will wake up the thread immediately if it is blocked inside
    // the select() system call.
    notificationSockets[0].close();
    // Wait for the select thread to terminate.
    Uninterruptibles.joinUninterruptibly(watcherThread);
  }

  @VisibleForTesting
  public boolean isClosed() {
    lock.lock();
    try {
      return closed;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Add a socket.
   *
   * @param sock     The socket to add.  It is an error to re-add a socket that
   *                   we are already watching.
   * @param handler  The handler to associate with this socket.  This may be
   *                   called any time after this function is called.
   */
  public void add(DomainSocket sock, Handler handler) {
    lock.lock();
    try {
      if (closed) {
        handler.handle(sock);
        IOUtils.cleanup(LOG, sock);
        return;
      }
      Entry entry = new Entry(sock, handler);
      try {
        sock.refCount.reference();
      } catch (ClosedChannelException e1) {
        // If the socket is already closed before we add it, invoke the
        // handler immediately.  Then we're done.
        handler.handle(sock);
        return;
      }
      toAdd.add(entry);
      kick();
      while (true) {
        try {
          processedCond.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (!toAdd.contains(entry)) {
          break;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove a socket.  Its handler will be called.
   *
   * @param sock     The socket to remove.
   */
  public void remove(DomainSocket sock) {
    lock.lock();
    try {
      if (closed) return;
      toRemove.put(sock.fd, sock);
      kick();
      while (true) {
        try {
          processedCond.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        if (!toRemove.containsKey(sock.fd)) {
          break;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Wake up the DomainSocketWatcher thread.
   */
  private void kick() {
    assert(lock.isHeldByCurrentThread());
    
    if (kicked) {
      return;
    }
    
    try {
      notificationSockets[0].getOutputStream().write(0);
      kicked = true;
    } catch (IOException e) {
      if (!closed) {
        LOG.error(this + ": error writing to notificationSockets[0]", e);
      }
    }
  }

  private void sendCallback(String caller, TreeMap<Integer, Entry> entries,
      FdSet fdSet, int fd) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + ": " + caller + " starting sendCallback for fd " + fd);
    }
    Entry entry = entries.get(fd);
    Preconditions.checkNotNull(entry,
        this + ": fdSet contained " + fd + ", which we were " +
        "not tracking.");
    DomainSocket sock = entry.getDomainSocket();
    if (entry.getHandler().handle(sock)) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(this + ": " + caller + ": closing fd " + fd +
            " at the request of the handler.");
      }
      if (toRemove.remove(fd) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(this + ": " + caller + " : sendCallback processed fd " +
            fd  + " in toRemove.");
        }
      }
      try {
        sock.refCount.unreferenceCheckClosed();
      } catch (IOException e) {
        Preconditions.checkArgument(false,
            this + ": file descriptor " + sock.fd + " was closed while " +
            "still in the poll(2) loop.");
      }
      IOUtils.cleanup(LOG, sock);
      entries.remove(fd);
      fdSet.remove(fd);
    } else {
      if (LOG.isTraceEnabled()) {
        LOG.trace(this + ": " + caller + ": sendCallback not " +
            "closing fd " + fd);
      }
    }
  }

  @VisibleForTesting
  final Thread watcherThread = new Thread(new Runnable() {
    @Override
    public void run() {
      if (LOG.isDebugEnabled()) {
        LOG.debug(this + ": starting with interruptCheckPeriodMs = " +
            interruptCheckPeriodMs);
      }
      final TreeMap<Integer, Entry> entries = new TreeMap<Integer, Entry>();
      FdSet fdSet = new FdSet();
      addNotificationSocket(entries, fdSet);
      try {
        while (true) {
          lock.lock();
          try {
            for (int fd : fdSet.getAndClearReadableFds()) {
              sendCallback("getAndClearReadableFds", entries, fdSet, fd);
            }
            if (!(toAdd.isEmpty() && toRemove.isEmpty())) {
              // Handle pending additions (before pending removes).
              for (Iterator<Entry> iter = toAdd.iterator(); iter.hasNext(); ) {
                Entry entry = iter.next();
                DomainSocket sock = entry.getDomainSocket();
                Entry prevEntry = entries.put(sock.fd, entry);
                Preconditions.checkState(prevEntry == null,
                    this + ": tried to watch a file descriptor that we " +
                    "were already watching: " + sock);
                if (LOG.isTraceEnabled()) {
                  LOG.trace(this + ": adding fd " + sock.fd);
                }
                fdSet.add(sock.fd);
                iter.remove();
              }
              // Handle pending removals
              while (true) {
                Map.Entry<Integer, DomainSocket> entry = toRemove.firstEntry();
                if (entry == null) break;
                sendCallback("handlePendingRemovals",
                    entries, fdSet, entry.getValue().fd);
              }
              processedCond.signalAll();
            }
            // Check if the thread should terminate.  Doing this check now is
            // easier than at the beginning of the loop, since we know toAdd and
            // toRemove are now empty and processedCond has been notified if it
            // needed to be.
            if (closed) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(toString() + " thread terminating.");
              }
              return;
            }
            // Check if someone sent our thread an InterruptedException while we
            // were waiting in poll().
            if (Thread.interrupted()) {
              throw new InterruptedException();
            }
          } finally {
            lock.unlock();
          }
          doPoll0(interruptCheckPeriodMs, fdSet);
        }
      } catch (InterruptedException e) {
        LOG.info(toString() + " terminating on InterruptedException");
      } catch (IOException e) {
        LOG.error(toString() + " terminating on IOException", e);
      } finally {
        lock.lock();
        try {
          kick(); // allow the handler for notificationSockets[0] to read a byte
          for (Entry entry : entries.values()) {
            sendCallback("close", entries, fdSet, entry.getDomainSocket().fd);
          }
          entries.clear();
          fdSet.close();
        } finally {
          lock.unlock();
        }
      }
    }
  });

  private void addNotificationSocket(final TreeMap<Integer, Entry> entries,
      FdSet fdSet) {
    entries.put(notificationSockets[1].fd, 
        new Entry(notificationSockets[1], new NotificationHandler()));
    try {
      notificationSockets[1].refCount.reference();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    fdSet.add(notificationSockets[1].fd);
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + ": adding notificationSocket " +
          notificationSockets[1].fd + ", connected to " +
          notificationSockets[0].fd);
    }
  }

  public String toString() {
    return "DomainSocketWatcher(" + System.identityHashCode(this) + ")"; 
  }

  private static native int doPoll0(int maxWaitMs, FdSet readFds)
      throws IOException;
}
