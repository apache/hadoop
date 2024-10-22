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

package org.apache.hadoop.conf;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Utility base class for implementing the Reconfigurable interface.
 *
 * Subclasses should override reconfigurePropertyImpl to change individual
 * properties and getReconfigurableProperties to get all properties that
 * can be changed at run time.
 */
public abstract class ReconfigurableBase 
  extends Configured implements Reconfigurable {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconfigurableBase.class);
  // Use for testing purpose.
  private ReconfigurationUtil reconfigurationUtil = new ReconfigurationUtil();

  /** Background thread to reload configuration. */
  private Thread reconfigThread = null;
  private volatile boolean shouldRun = true;
  private Object reconfigLock = new Object();

  /**
   * The timestamp when the <code>reconfigThread</code> starts.
   */
  private long startTime = 0;

  /**
   * The timestamp when the <code>reconfigThread</code> finishes.
   */
  private long endTime = 0;

  /**
   * A map of <changed property, error message>. If error message is present,
   * it contains the messages about the error occurred when applies the particular
   * change. Otherwise, it indicates that the change has been successfully applied.
   */
  private Map<PropertyChange, Optional<String>> status = null;

  /**
   * Construct a ReconfigurableBase.
   */
  public ReconfigurableBase() {
    super(new Configuration());
  }

  /**
   * Construct a ReconfigurableBase with the {@link Configuration}
   * conf.
   * @param conf configuration.
   */
  public ReconfigurableBase(Configuration conf) {
    super((conf == null) ? new Configuration() : conf);
  }

  @VisibleForTesting
  public void setReconfigurationUtil(ReconfigurationUtil ru) {
    reconfigurationUtil = Preconditions.checkNotNull(ru);
  }

  /**
   * Create a new configuration.
   * @return configuration.
   */
  protected abstract Configuration getNewConf();

  @VisibleForTesting
  public Collection<PropertyChange> getChangedProperties(
      Configuration newConf, Configuration oldConf) {
    return reconfigurationUtil.parseChangedProperties(newConf, oldConf);
  }

  /**
   * A background thread to apply configuration changes.
   */
  private static class ReconfigurationThread extends Thread {
    private ReconfigurableBase parent;

    ReconfigurationThread(ReconfigurableBase base) {
      this.parent = base;
    }

    // See {@link ReconfigurationServlet#applyChanges}
    public void run() {
      LOG.info("Starting reconfiguration task.");
      final Configuration oldConf = parent.getConf();
      final Configuration newConf = parent.getNewConf();
      final Collection<PropertyChange> changes =
          parent.getChangedProperties(newConf, oldConf);
      Map<PropertyChange, Optional<String>> results = Maps.newHashMap();
      ConfigRedactor oldRedactor = new ConfigRedactor(oldConf);
      ConfigRedactor newRedactor = new ConfigRedactor(newConf);
      for (PropertyChange change : changes) {
        String errorMessage = null;
        String oldValRedacted = oldRedactor.redact(change.prop, change.oldVal);
        String newValRedacted = newRedactor.redact(change.prop, change.newVal);
        if (!parent.isPropertyReconfigurable(change.prop)) {
          LOG.info(String.format(
              "Property %s is not configurable: old value: %s, new value: %s",
              change.prop,
              oldValRedacted,
              newValRedacted));
          continue;
        }
        LOG.info("Change property: " + change.prop + " from \""
            + ((change.oldVal == null) ? "<default>" : oldValRedacted)
            + "\" to \""
            + ((change.newVal == null) ? "<default>" : newValRedacted)
            + "\".");
        try {
          String effectiveValue =
              parent.reconfigurePropertyImpl(change.prop, change.newVal);
          if (change.newVal != null) {
            oldConf.set(change.prop, effectiveValue);
          } else {
            oldConf.unset(change.prop);
          }
        } catch (ReconfigurationException e) {
          Throwable cause = e.getCause();
          errorMessage = cause == null ? e.getMessage() : cause.getMessage();
        }
        results.put(change, Optional.ofNullable(errorMessage));
      }

      synchronized (parent.reconfigLock) {
        parent.endTime = Time.now();
        parent.status = Collections.unmodifiableMap(results);
        parent.reconfigThread = null;
      }
    }
  }

  /**
   * Start a reconfiguration task to reload configuration in background.
   * @throws IOException raised on errors performing I/O.
   */
  public void startReconfigurationTask() throws IOException {
    synchronized (reconfigLock) {
      if (!shouldRun) {
        String errorMessage = "The server is stopped.";
        LOG.warn(errorMessage);
        throw new IOException(errorMessage);
      }
      if (reconfigThread != null) {
        String errorMessage = "Another reconfiguration task is running.";
        LOG.warn(errorMessage);
        throw new IOException(errorMessage);
      }
      reconfigThread = new ReconfigurationThread(this);
      reconfigThread.setDaemon(true);
      reconfigThread.setName("Reconfiguration Task");
      reconfigThread.start();
      startTime = Time.now();
    }
  }

  public ReconfigurationTaskStatus getReconfigurationTaskStatus() {
    synchronized (reconfigLock) {
      if (reconfigThread != null) {
        return new ReconfigurationTaskStatus(startTime, 0, null);
      }
      return new ReconfigurationTaskStatus(startTime, endTime, status);
    }
  }

  public void shutdownReconfigurationTask() {
    Thread tempThread;
    synchronized (reconfigLock) {
      shouldRun = false;
      if (reconfigThread == null) {
        return;
      }
      tempThread = reconfigThread;
      reconfigThread = null;
    }

    try {
      tempThread.join();
    } catch (InterruptedException e) {
    }
  }

  /**
   * {@inheritDoc}
   *
   * This method makes the change to this objects {@link Configuration}
   * and calls reconfigurePropertyImpl to update internal data structures.
   * This method cannot be overridden, subclasses should instead override
   * reconfigurePropertyImpl.
   */
  @Override
  public final void reconfigureProperty(String property, String newVal)
    throws ReconfigurationException {
    if (isPropertyReconfigurable(property)) {
      synchronized(getConf()) {
        getConf().get(property);
        String effectiveValue = reconfigurePropertyImpl(property, newVal);
        if (effectiveValue != null) {
          LOG.info("changing property " + property + " to " + effectiveValue);
          getConf().set(property, effectiveValue);
        } else {
          getConf().unset(property);
        }
      }
    } else {
      throw new ReconfigurationException(property, newVal,
                                             getConf().get(property));
    }
  }

  /**
   * {@inheritDoc}
   *
   * Subclasses must override this.
   */
  @Override 
  public abstract Collection<String> getReconfigurableProperties();


  /**
   * {@inheritDoc}
   *
   * Subclasses may wish to override this with a more efficient implementation.
   */
  @Override
  public boolean isPropertyReconfigurable(String property) {
    return getReconfigurableProperties().contains(property);
  }

  /**
   * Change a configuration property.
   *
   * Subclasses must override this. This method applies the change to
   * all internal data structures derived from the configuration property
   * that is being changed. If this object owns other Reconfigurable objects
   * reconfigureProperty should be called recursively to make sure that
   * the configuration of these objects are updated.
   *
   * @param property Name of the property that is being reconfigured.
   * @param newVal Proposed new value of the property.
   * @return Effective new value of the property. This may be different from
   *         newVal.
   *
   * @throws ReconfigurationException if there was an error applying newVal.
   */
  protected abstract String reconfigurePropertyImpl(
      String property, String newVal) throws ReconfigurationException;

}
