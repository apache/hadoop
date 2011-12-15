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

package org.apache.hadoop.hdfs.security.token.delegation;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A daemon thread that waits for the next file system to renew.
 */
@InterfaceAudience.Private
public class DelegationTokenRenewer<T extends FileSystem & DelegationTokenRenewer.Renewable>
    extends Thread {
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
  private static class RenewAction<T extends FileSystem & Renewable>
      implements Delayed {
    /** when should the renew happen */
    private long renewalTime;
    /** a weak reference to the file system so that it can be garbage collected */
    private final WeakReference<T> weakFs;

    private RenewAction(final T fs) {
      this.weakFs = new WeakReference<T>(fs);
      updateRenewalTime();
    }
 
    /** Get the delay until this event should happen. */
    @Override
    public long getDelay(final TimeUnit unit) {
      final long millisLeft = renewalTime - System.currentTimeMillis();
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
      return (int)renewalTime ^ (int)(renewalTime >>> 32);
    }

    @Override
    public boolean equals(final Object that) {
      if (that == null || !(that instanceof RenewAction)) {
        return false;
      }
      return compareTo((Delayed)that) == 0;
    }
    
    /**
     * Set a new time for the renewal.
     * It can only be called when the action is not in the queue.
     * @param newTime the new time
     */
    private void updateRenewalTime() {
      renewalTime = RENEW_CYCLE + System.currentTimeMillis();
    }

    /**
     * Renew or replace the delegation token for this file system.
     * @return
     * @throws IOException
     */
    private boolean renew() throws IOException, InterruptedException {
      final T fs = weakFs.get();
      final boolean b = fs != null;
      if (b) {
        synchronized(fs) {
          try {
            fs.getRenewToken().renew(fs.getConf());
          } catch (IOException ie) {
            try {
              fs.setDelegationToken(fs.getDelegationToken(null));
            } catch (IOException ie2) {
              throw new IOException("Can't renew or get new delegation token ", ie);
            }
          }
        }
      }
      return b;
    }

    @Override
    public String toString() {
      Renewable fs = weakFs.get();
      return fs == null? "evaporated token renew"
          : "The token will be renewed in " + getDelay(TimeUnit.SECONDS)
            + " secs, renewToken=" + fs.getRenewToken();
    }
  }

  /** Wait for 95% of a day between renewals */
  private static final int RENEW_CYCLE = 24 * 60 * 60 * 950;

  private DelayQueue<RenewAction<T>> queue = new DelayQueue<RenewAction<T>>();

  public DelegationTokenRenewer(final Class<T> clazz) {
    super(clazz.getSimpleName() + "-" + DelegationTokenRenewer.class.getSimpleName());
    setDaemon(true);
  }

  @Override
  public void start() {
    return; // lazy start when addRenewAction is actually called
  }
  
  /** Add a renew action to the queue. */
  public void addRenewAction(final T fs) {
    queue.add(new RenewAction<T>(fs));
    if (!isAlive()) super.start();
  }

  @Override
  public void run() {
    for(;;) {
      RenewAction<T> action = null;
      try {
        action = queue.take();
        if (action.renew()) {
          action.updateRenewalTime();
          queue.add(action);
        }
      } catch (InterruptedException ie) {
        return;
      } catch (Exception ie) {
        T.LOG.warn("Failed to renew token, action=" + action, ie);
      }
    }
  }
}