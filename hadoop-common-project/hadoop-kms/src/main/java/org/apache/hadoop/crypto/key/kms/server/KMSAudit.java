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

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides convenience methods for audit logging consistently the different
 * types of events.
 */
public class KMSAudit {

  private static class AuditEvent {
    private final AtomicLong accessCount = new AtomicLong(-1);
    private final String keyName;
    private final String user;
    private final KMS.KMSOp op;
    private final String extraMsg;
    private final long startTime = System.currentTimeMillis();

    private AuditEvent(String keyName, String user, KMS.KMSOp op, String msg) {
      this.keyName = keyName;
      this.user = user;
      this.op = op;
      this.extraMsg = msg;
    }

    public String getExtraMsg() {
      return extraMsg;
    }

    public AtomicLong getAccessCount() {
      return accessCount;
    }

    public String getKeyName() {
      return keyName;
    }

    public String getUser() {
      return user;
    }

    public KMS.KMSOp getOp() {
      return op;
    }

    public long getStartTime() {
      return startTime;
    }
  }

  public static enum OpStatus {
    OK, UNAUTHORIZED, UNAUTHENTICATED, ERROR;
  }

  private static Set<KMS.KMSOp> AGGREGATE_OPS_WHITELIST = Sets.newHashSet(
    KMS.KMSOp.GET_KEY_VERSION, KMS.KMSOp.GET_CURRENT_KEY,
    KMS.KMSOp.DECRYPT_EEK, KMS.KMSOp.GENERATE_EEK
  );

  private Cache<String, AuditEvent> cache;

  private ScheduledExecutorService executor;

  public static final String KMS_LOGGER_NAME = "kms-audit";

  private static Logger AUDIT_LOG = LoggerFactory.getLogger(KMS_LOGGER_NAME);

  /**
   * Create a new KMSAudit.
   *
   * @param windowMs Duplicate events within the aggregation window are quashed
   *                 to reduce log traffic. A single message for aggregated
   *                 events is printed at the end of the window, along with a
   *                 count of the number of aggregated events.
   */
  KMSAudit(long windowMs) {
    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(windowMs, TimeUnit.MILLISECONDS)
        .removalListener(
            new RemovalListener<String, AuditEvent>() {
              @Override
              public void onRemoval(
                  RemovalNotification<String, AuditEvent> entry) {
                AuditEvent event = entry.getValue();
                if (event.getAccessCount().get() > 0) {
                  KMSAudit.this.logEvent(event);
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
  }

  private void logEvent(AuditEvent event) {
    AUDIT_LOG.info(
        "OK[op={}, key={}, user={}, accessCount={}, interval={}ms] {}",
        event.getOp(), event.getKeyName(), event.getUser(),
        event.getAccessCount().get(),
        (System.currentTimeMillis() - event.getStartTime()),
        event.getExtraMsg());
  }

  private void op(OpStatus opStatus, final KMS.KMSOp op, final String user,
      final String key, final String extraMsg) {
    if (!Strings.isNullOrEmpty(user) && !Strings.isNullOrEmpty(key)
        && (op != null)
        && AGGREGATE_OPS_WHITELIST.contains(op)) {
      String cacheKey = createCacheKey(user, key, op);
      if (opStatus == OpStatus.UNAUTHORIZED) {
        cache.invalidate(cacheKey);
        AUDIT_LOG.info("UNAUTHORIZED[op={}, key={}, user={}] {}", op, key, user,
            extraMsg);
      } else {
        try {
          AuditEvent event = cache.get(cacheKey, new Callable<AuditEvent>() {
            @Override
            public AuditEvent call() throws Exception {
              return new AuditEvent(key, user, op, extraMsg);
            }
          });
          // Log first access (initialized as -1 so
          // incrementAndGet() == 0 implies first access)
          if (event.getAccessCount().incrementAndGet() == 0) {
            event.getAccessCount().incrementAndGet();
            logEvent(event);
          }
        } catch (ExecutionException ex) {
          throw new RuntimeException(ex);
        }
      }
    } else {
      List<String> kvs = new LinkedList<String>();
      if (op != null) {
        kvs.add("op=" + op);
      }
      if (!Strings.isNullOrEmpty(key)) {
        kvs.add("key=" + key);
      }
      if (!Strings.isNullOrEmpty(user)) {
        kvs.add("user=" + user);
      }
      if (kvs.size() == 0) {
        AUDIT_LOG.info("{} {}", opStatus.toString(), extraMsg);
      } else {
        String join = Joiner.on(", ").join(kvs);
        AUDIT_LOG.info("{}[{}] {}", opStatus.toString(), join, extraMsg);
      }
    }
  }

  public void ok(UserGroupInformation user, KMS.KMSOp op, String key,
      String extraMsg) {
    op(OpStatus.OK, op, user.getShortUserName(), key, extraMsg);
  }

  public void ok(UserGroupInformation user, KMS.KMSOp op, String extraMsg) {
    op(OpStatus.OK, op, user.getShortUserName(), null, extraMsg);
  }

  public void unauthorized(UserGroupInformation user, KMS.KMSOp op, String key) {
    op(OpStatus.UNAUTHORIZED, op, user.getShortUserName(), key, "");
  }

  public void error(UserGroupInformation user, String method, String url,
      String extraMsg) {
    op(OpStatus.ERROR, null, user.getShortUserName(), null, "Method:'" + method
        + "' Exception:'" + extraMsg + "'");
  }

  public void unauthenticated(String remoteHost, String method,
      String url, String extraMsg) {
    op(OpStatus.UNAUTHENTICATED, null, null, null, "RemoteHost:"
        + remoteHost + " Method:" + method
        + " URL:" + url + " ErrorMsg:'" + extraMsg + "'");
  }

  private static String createCacheKey(String user, String key, KMS.KMSOp op) {
    return user + "#" + key + "#" + op;
  }

  public void shutdown() {
    executor.shutdownNow();
  }
}
