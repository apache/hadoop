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

package org.apache.hadoop.lib.service.scheduler;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.lib.lang.RunnableCallable;
import org.apache.hadoop.lib.server.BaseService;
import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.lib.server.ServiceException;
import org.apache.hadoop.lib.service.Instrumentation;
import org.apache.hadoop.lib.service.Scheduler;
import org.apache.hadoop.lib.util.Check;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@InterfaceAudience.Private
public class SchedulerService extends BaseService implements Scheduler {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerService.class);

  private static final String INST_GROUP = "scheduler";

  public static final String PREFIX = "scheduler";

  public static final String CONF_THREADS = "threads";

  private ScheduledExecutorService scheduler;

  public SchedulerService() {
    super(PREFIX);
  }

  @Override
  public void init() throws ServiceException {
    int threads = getServiceConfig().getInt(CONF_THREADS, 5);
    scheduler = new ScheduledThreadPoolExecutor(threads);
    LOG.debug("Scheduler started");
  }

  @Override
  public void destroy() {
    try {
      long limit = Time.now() + 30 * 1000;
      scheduler.shutdownNow();
      while (!scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
        LOG.debug("Waiting for scheduler to shutdown");
        if (Time.now() > limit) {
          LOG.warn("Gave up waiting for scheduler to shutdown");
          break;
        }
      }
      if (scheduler.isTerminated()) {
        LOG.debug("Scheduler shutdown");
      }
    } catch (InterruptedException ex) {
      LOG.warn(ex.getMessage(), ex);
    }
  }

  @Override
  public Class[] getServiceDependencies() {
    return new Class[]{Instrumentation.class};
  }

  @Override
  public Class getInterface() {
    return Scheduler.class;
  }

  @Override
  public void schedule(final Callable<?> callable, long delay, long interval, TimeUnit unit) {
    Check.notNull(callable, "callable");
    if (!scheduler.isShutdown()) {
      LOG.debug("Scheduling callable [{}], interval [{}] seconds, delay [{}] in [{}]",
                new Object[]{callable, delay, interval, unit});
      Runnable r = new Runnable() {
        @Override
        public void run() {
          String instrName = callable.getClass().getSimpleName();
          Instrumentation instr = getServer().get(Instrumentation.class);
          if (getServer().getStatus() == Server.Status.HALTED) {
            LOG.debug("Skipping [{}], server status [{}]", callable, getServer().getStatus());
            instr.incr(INST_GROUP, instrName + ".skips", 1);
          } else {
            LOG.debug("Executing [{}]", callable);
            instr.incr(INST_GROUP, instrName + ".execs", 1);
            Instrumentation.Cron cron = instr.createCron().start();
            try {
              callable.call();
            } catch (Exception ex) {
              instr.incr(INST_GROUP, instrName + ".fails", 1);
              LOG.error("Error executing [{}], {}", new Object[]{callable, ex.getMessage(), ex});
            } finally {
              instr.addCron(INST_GROUP, instrName, cron.stop());
            }
          }
        }
      };
      scheduler.scheduleWithFixedDelay(r, delay, interval, unit);
    } else {
      throw new IllegalStateException(
        MessageFormat.format("Scheduler shutting down, ignoring scheduling of [{}]", callable));
    }
  }

  @Override
  public void schedule(Runnable runnable, long delay, long interval, TimeUnit unit) {
    schedule((Callable<?>) new RunnableCallable(runnable), delay, interval, unit);
  }

}
