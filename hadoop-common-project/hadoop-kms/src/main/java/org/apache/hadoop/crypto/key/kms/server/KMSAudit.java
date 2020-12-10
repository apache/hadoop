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
package org.apache.hadoop.crypto.key.kms.server;

import static org.apache.hadoop.crypto.key.kms.server.KMSAuditLogger.AuditEvent;
import static org.apache.hadoop.crypto.key.kms.server.KMSAuditLogger.OpStatus;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.server.KMSACLs.Type;
import org.apache.hadoop.crypto.key.kms.server.KeyAuthorizationKeyProvider.KeyOpType;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalListener;
import org.apache.hadoop.thirdparty.com.google.common.cache.RemovalNotification;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Provides convenience methods for audit logging consisting different
 * types of events.
 */
public class KMSAudit {
  @VisibleForTesting
  static final Set<KMS.KMSOp> AGGREGATE_OPS_WHITELIST = Sets.newHashSet(
      KMS.KMSOp.GET_KEY_VERSION, KMS.KMSOp.GET_CURRENT_KEY,
      KMS.KMSOp.DECRYPT_EEK, KMS.KMSOp.GENERATE_EEK, KMS.KMSOp.REENCRYPT_EEK
  );

  private Cache<String, AuditEvent> cache;

  private ScheduledExecutorService executor;

  public static final String KMS_LOGGER_NAME = "kms-audit";

  private final static Logger LOG = LoggerFactory.getLogger(KMSAudit.class);
  private final List<KMSAuditLogger> auditLoggers = new LinkedList<>();

  /**
   * Create a new KMSAudit.
   *
   * @param conf The configuration object.
   */
  KMSAudit(Configuration conf) {
    // Duplicate events within the aggregation window are quashed
    // to reduce log traffic. A single message for aggregated
    // events is printed at the end of the window, along with a
    // count of the number of aggregated events.
    long windowMs = conf.getLong(KMSConfiguration.KMS_AUDIT_AGGREGATION_WINDOW,
        KMSConfiguration.KMS_AUDIT_AGGREGATION_WINDOW_DEFAULT);
    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(windowMs, TimeUnit.MILLISECONDS)
        .removalListener(
            new RemovalListener<String, AuditEvent>() {
              @Override
              public void onRemoval(
                  RemovalNotification<String, AuditEvent> entry) {
                AuditEvent event = entry.getValue();
                if (event.getAccessCount().get() > 0) {
                  KMSAudit.this.logEvent(OpStatus.OK, event);
                  event.getAccessCount().set(0);
                  KMSAudit.this.cache.put(entry.getKey(), event);
                }
              }
            }).build();
    executor = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat(KMS_LOGGER_NAME + "_thread").build());
    executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        cache.cleanUp();
      }
    }, windowMs / 10, windowMs / 10, TimeUnit.MILLISECONDS);
    initializeAuditLoggers(conf);
  }

  /**
   * Read the KMSAuditLogger classes from configuration. If any loggers fail to
   * load, a RumTimeException will be thrown.
   *
   * @param conf The configuration.
   * @return Collection of KMSAudigLogger classes.
   */
  private Set<Class<? extends KMSAuditLogger>> getAuditLoggerClasses(
      final Configuration conf) {
    Set<Class<? extends KMSAuditLogger>> result = new HashSet<>();
    // getTrimmedStringCollection will remove duplicates.
    Collection<String> classes =
        conf.getTrimmedStringCollection(KMSConfiguration.KMS_AUDIT_LOGGER_KEY);
    if (classes.isEmpty()) {
      LOG.info("No audit logger configured, using default.");
      result.add(SimpleKMSAuditLogger.class);
      return result;
    }

    for (String c : classes) {
      try {
        Class<?> cls = conf.getClassByName(c);
        result.add(cls.asSubclass(KMSAuditLogger.class));
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException("Failed to load " + c + ", please check "
            + "configuration " + KMSConfiguration.KMS_AUDIT_LOGGER_KEY, cnfe);
      }
    }
    return result;
  }

  /**
   * Create a collection of KMSAuditLoggers from configuration, and initialize
   * them. If any logger failed to be created or initialized, a RunTimeException
   * is thrown.
   */
  private void initializeAuditLoggers(Configuration conf) {
    Set<Class<? extends KMSAuditLogger>> classes = getAuditLoggerClasses(conf);
    Preconditions
        .checkState(!classes.isEmpty(), "Should have at least 1 audit logger.");
    for (Class<? extends KMSAuditLogger> c : classes) {
      final KMSAuditLogger logger = ReflectionUtils.newInstance(c, conf);
      auditLoggers.add(logger);
    }
    for (KMSAuditLogger logger: auditLoggers) {
      try {
        LOG.info("Initializing audit logger {}", logger.getClass());
        logger.initialize(conf);
      } catch (Exception ex) {
        throw new RuntimeException(
            "Failed to initialize " + logger.getClass().getName(), ex);
      }
    }
  }

  private void logEvent(final OpStatus status, AuditEvent event) {
    event.setEndTime(Time.now());
    for (KMSAuditLogger logger: auditLoggers) {
      logger.logAuditEvent(status, event);
    }
  }

  /**
   * Logs to the audit service a single operation on the KMS or on a key.
   *
   * @param opStatus
   *          The outcome of the audited event
   * @param op
   *          The operation being audited (either {@link KMS.KMSOp} or
   *          {@link Type} N.B this is passed as an {@link Object} to allow
   *          either enum to be passed in.
   * @param ugi
   *          The user's security context
   * @param key
   *          The String name of the key if applicable
   * @param remoteHost
   *          The hostname of the requesting service
   * @param extraMsg
   *          Any extra details for auditing
   */
  private void op(final OpStatus opStatus, final Object op,
      final UserGroupInformation ugi, final String key, final String remoteHost,
      final String extraMsg) {
    final String user = ugi == null ? null: ugi.getUserName();
    if (!Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(key)
        && (op != null)
        && AGGREGATE_OPS_WHITELIST.contains(op)) {
      String cacheKey = createCacheKey(user, key, op);
      if (opStatus == OpStatus.UNAUTHORIZED) {
        cache.invalidate(cacheKey);
        logEvent(opStatus, new AuditEvent(op, ugi, key, remoteHost, extraMsg));
      } else {
        try {
          AuditEvent event = cache.get(cacheKey, new Callable<AuditEvent>() {
            @Override
            public AuditEvent call() throws Exception {
              return new AuditEvent(op, ugi, key, remoteHost, extraMsg);
            }
          });
          // Log first access (initialized as -1 so
          // incrementAndGet() == 0 implies first access)
          if (event.getAccessCount().incrementAndGet() == 0) {
            event.getAccessCount().incrementAndGet();
            logEvent(opStatus, event);
          }
        } catch (ExecutionException ex) {
          throw new RuntimeException(ex);
        }
      }
    } else {
      logEvent(opStatus, new AuditEvent(op, ugi, key, remoteHost, extraMsg));
    }
  }

  public void ok(UserGroupInformation user, KMS.KMSOp op, String key,
      String extraMsg) {
    op(OpStatus.OK, op, user, key, "Unknown", extraMsg);
  }

  public void ok(UserGroupInformation user, KMS.KMSOp op, String extraMsg) {
    op(OpStatus.OK, op, user, null, "Unknown", extraMsg);
  }

  public void unauthorized(UserGroupInformation user, KMS.KMSOp op, String key) {
    op(OpStatus.UNAUTHORIZED, op, user, key, "Unknown", "");
  }

  public void unauthorized(UserGroupInformation user, KeyOpType op,
      String key) {
    op(OpStatus.UNAUTHORIZED, op, user, key, "Unknown", "");

  }

  public void error(UserGroupInformation user, String method, String url,
      String extraMsg) {
    op(OpStatus.ERROR, null, user, null, "Unknown", "Method:'" + method
        + "' Exception:'" + extraMsg + "'");
  }

  public void unauthenticated(String remoteHost, String method,
      String url, String extraMsg) {
    op(OpStatus.UNAUTHENTICATED, null, null, null, remoteHost, "RemoteHost:"
        + remoteHost + " Method:" + method
        + " URL:" + url + " ErrorMsg:'" + extraMsg + "'");
  }

  private static String createCacheKey(String user, String key, Object op) {
    return user + "#" + key + "#" + op;
  }

  public void shutdown() {
    executor.shutdownNow();
    for (KMSAuditLogger logger : auditLoggers) {
      try {
        logger.cleanup();
      } catch (Exception ex) {
        LOG.error("Failed to cleanup logger {}", logger.getClass(), ex);
      }
    }
  }

  @VisibleForTesting
  void evictCacheForTesting() {
    cache.invalidateAll();
  }
}
