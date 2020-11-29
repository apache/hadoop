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

package org.apache.hadoop.fs;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A daemon thread that waits for the next file system to renew.
 */
@InterfaceAudience.Private
public class DelegationTokenRenewer
    extends Thread {
  private static final Logger LOG = LoggerFactory
      .getLogger(DelegationTokenRenewer.class);

  /** The renewable interface used by the renewer. */
  public interface Renewable {
    /** @return the renew token. */
    public Token<?> getRenewToken();

    /** Set delegation token. */
    public <T extends TokenIdentifier> void setDelegationToken(Token<T> token);
  }

  /**
   * An action that will renew and replace the file system's delegation 
   * tokens automatically.
   */
  public static class RenewAction<T extends FileSystem & Renewable>
      implements Delayed {
    /** when should the renew happen */
    private long renewalTime;
    /** a weak reference to the file system so that it can be garbage collected */
    private final WeakReference<T> weakFs;
    private Token<?> token; 
    boolean isValid = true;

    private RenewAction(final T fs) {
      this.weakFs = new WeakReference<T>(fs);
      this.token = fs.getRenewToken();
      updateRenewalTime(renewCycle);
    }
 
    public boolean isValid() {
      return isValid;
    }
    
    /** Get the delay until this event should happen. */
    @Override
    public long getDelay(final TimeUnit unit) {
      final long millisLeft = renewalTime - Time.now();
      return unit.convert(millisLeft, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(final Delayed delayed) {
      final RenewAction<?> that = (RenewAction<?>)delayed;
      return this.renewalTime < that.renewalTime? -1
          : this.renewalTime == that.renewalTime? 0: 1;
    }

    @Override
    public int hashCode() {
      return token.hashCode();
    }

    @Override
    public boolean equals(final Object that) {
      if (this == that) {
        return true;
      } else if (that == null || !(that instanceof RenewAction)) {
        return false;
      }
      return token.equals(((RenewAction<?>)that).token);
    }

    /**
     * Set a new time for the renewal.
     * It can only be called when the action is not in the queue or any
     * collection because the hashCode may change
     * @param newTime the new time
     */
    private void updateRenewalTime(long delay) {
      renewalTime = Time.now() + delay - delay/10;
    }

    /**
     * Renew or replace the delegation token for this file system.
     * It can only be called when the action is not in the queue.
     * @return
     * @throws IOException
     */
    private boolean renew() throws IOException, InterruptedException {
      final T fs = weakFs.get();
      final boolean b = fs != null;
      if (b) {
        synchronized(fs) {
          try {
            long expires = token.renew(fs.getConf());
            updateRenewalTime(expires - Time.now());
          } catch (IOException ie) {
            try {
              Token<?>[] tokens = fs.addDelegationTokens(null, null);
              if (tokens.length == 0) {
                throw new IOException("addDelegationTokens returned no tokens");
              }
              token = tokens[0];
              updateRenewalTime(renewCycle);
              fs.setDelegationToken(token);
            } catch (IOException ie2) {
              isValid = false;
              throw new IOException("Can't renew or get new delegation token ", ie);
            }
          }
        }
      }
      return b;
    }

    private void cancel() throws IOException, InterruptedException {
      final T fs = weakFs.get();
      if (fs != null) {
        token.cancel(fs.getConf());
      }
    }

    @Override
    public String toString() {
      Renewable fs = weakFs.get();
      return fs == null? "evaporated token renew"
          : "The token will be renewed in " + getDelay(TimeUnit.SECONDS)
            + " secs, renewToken=" + token;
    }
  }

  /** assumes renew cycle for a token is 24 hours... */
  private static final long RENEW_CYCLE = 24 * 60 * 60 * 1000; 

  @InterfaceAudience.Private
  @VisibleForTesting
  public static long renewCycle = RENEW_CYCLE;

  /** Queue to maintain the RenewActions to be processed by the {@link #run()} */
  private volatile DelayQueue<RenewAction<?>> queue = new DelayQueue<RenewAction<?>>();
  
  /** For testing purposes */
  @VisibleForTesting
  protected int getRenewQueueLength() {
    return queue.size();
  }

  /**
   * Create the singleton instance. However, the thread can be started lazily in
   * {@link #addRenewAction(FileSystem)}
   */
  private static DelegationTokenRenewer INSTANCE = null;

  private DelegationTokenRenewer(final Class<? extends FileSystem> clazz) {
    super(clazz.getSimpleName() + "-" + DelegationTokenRenewer.class.getSimpleName());
    setDaemon(true);
  }

  public static synchronized DelegationTokenRenewer getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new DelegationTokenRenewer(FileSystem.class);
    }
    return INSTANCE;
  }

  @VisibleForTesting
  static synchronized void reset() {
    if (INSTANCE != null) {
      INSTANCE.queue.clear();
      INSTANCE.interrupt();
      try {
        INSTANCE.join();
      } catch (InterruptedException e) {
        LOG.warn("Failed to reset renewer");
      } finally {
        INSTANCE = null;
      }
    }
  }
  
  /** Add a renew action to the queue. */
  @SuppressWarnings("static-access")
  public <T extends FileSystem & Renewable> RenewAction<T> addRenewAction(final T fs) {
    synchronized (this) {
      if (!isAlive()) {
        start();
      }
    }
    RenewAction<T> action = new RenewAction<T>(fs);
    if (action.token != null) {
      queue.add(action);
    } else {
      fs.LOG.error("does not have a token for renewal");
    }
    return action;
  }

  /**
   * Remove the associated renew action from the queue
   * 
   * @throws IOException
   */
  public <T extends FileSystem & Renewable> void removeRenewAction(
      final T fs) throws IOException {
    RenewAction<T> action = new RenewAction<T>(fs);
    if (queue.remove(action)) {
      try {
        action.cancel();
      } catch (InterruptedException ie) {
        LOG.error("Interrupted while canceling token for " + fs.getUri()
            + "filesystem");
        LOG.debug("Exception in removeRenewAction: {}", ie);
      }
    }
  }

  @SuppressWarnings("static-access")
  @Override
  public void run() {
    for(;;) {
      RenewAction<?> action = null;
      try {
        action = queue.take();
        if (action.renew()) {
          queue.add(action);
        }
      } catch (InterruptedException ie) {
        return;
      } catch (Exception ie) {
        action.weakFs.get().LOG.warn("Failed to renew token, action=" + action,
            ie);
      }
    }
  }
}
