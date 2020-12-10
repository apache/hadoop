/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.hadoop.security.authentication.util;

import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletContext;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract SignerSecretProvider that can be use used as the base for a
 * rolling secret.  The secret will roll over at the same interval as the token
 * validity, so there are only ever a maximum of two valid secrets at any
 * given time.  This class handles storing and returning the secrets, as well
 * as the rolling over.  At a minimum, subclasses simply need to implement the
 * generateNewSecret() method.  More advanced implementations can override
 * other methods to provide more advanced behavior, but should be careful when
 * doing so.
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public abstract class RolloverSignerSecretProvider
    extends SignerSecretProvider {

  @VisibleForTesting
  static Logger LOG = LoggerFactory.getLogger(
    RolloverSignerSecretProvider.class);
  /**
   * Stores the currently valid secrets.  The current secret is the 0th element
   * in the array.
   */
  private volatile byte[][] secrets;
  private ScheduledExecutorService scheduler;
  private boolean schedulerRunning;
  private boolean isDestroyed;

  public RolloverSignerSecretProvider() {
    schedulerRunning = false;
    isDestroyed = false;
  }

  /**
   * Initialize the SignerSecretProvider.  It initializes the current secret
   * and starts the scheduler for the rollover to run at an interval of
   * tokenValidity.
   * @param config configuration properties
   * @param servletContext servlet context
   * @param tokenValidity The amount of time a token is valid for
   * @throws Exception thrown if an error occurred
   */
  @Override
  public void init(Properties config, ServletContext servletContext,
          long tokenValidity) throws Exception {
    initSecrets(generateNewSecret(), null);
    startScheduler(tokenValidity, tokenValidity);
  }

  /**
   * Initializes the secrets array.  This should typically be called only once,
   * during init but some implementations may wish to call it other times.
   * previousSecret can be null if there isn't a previous secret, but
   * currentSecret should never be null.
   * @param currentSecret The current secret
   * @param previousSecret The previous secret
   */
  protected void initSecrets(byte[] currentSecret, byte[] previousSecret) {
    secrets = new byte[][]{currentSecret, previousSecret};
  }

  /**
   * Starts the scheduler for the rollover to run at an interval.
   * @param initialDelay The initial delay in the rollover in milliseconds
   * @param period The interval for the rollover in milliseconds
   */
  protected synchronized void startScheduler(long initialDelay, long period) {
    if (!schedulerRunning) {
      schedulerRunning = true;
      scheduler = Executors.newSingleThreadScheduledExecutor();
      scheduler.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          rollSecret();
        }
      }, initialDelay, period, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public synchronized void destroy() {
    if (!isDestroyed) {
      isDestroyed = true;
      if (scheduler != null) {
        scheduler.shutdown();
      }
      schedulerRunning = false;
      super.destroy();
    }
  }

  /**
   * Rolls the secret.  It is called automatically at the rollover interval.
   */
  protected synchronized void rollSecret() {
    if (!isDestroyed) {
      LOG.debug("rolling secret");
      byte[] newSecret = generateNewSecret();
      secrets = new byte[][]{newSecret, secrets[0]};
    }
  }

  /**
   * Subclasses should implement this to return a new secret.  It will be called
   * automatically at the secret rollover interval. It should never return null.
   * @return a new secret
   */
  protected abstract byte[] generateNewSecret();

  @Override
  public byte[] getCurrentSecret() {
    return secrets[0];
  }

  @Override
  public byte[][] getAllSecrets() {
    return secrets;
  }
}
