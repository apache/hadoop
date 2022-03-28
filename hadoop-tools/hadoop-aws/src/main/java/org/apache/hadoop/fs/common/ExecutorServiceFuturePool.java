/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.common;

import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

public class ExecutorServiceFuturePool {
    private ExecutorService executor;
    private final boolean interruptible = false;

    public ExecutorServiceFuturePool(ExecutorService executor) {
        this.executor = executor;
    }

    public Future<Void> apply(final Supplier<Void> f) {
        return executor.submit(f::get);
    }

    public String toString() {
        return String.format(Locale.ROOT,"ExecutorServiceFuturePool(interruptible=%s, executor=%s)",
                interruptible, executor);
    }

    public int poolSize() {
        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor)executor).getPoolSize();
        } else {
            return -1;
        }
    }

    public int numActiveTasks() {
        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor)executor).getActiveCount();
        } else {
            return -1;
        }
    }

    public long numCompletedTasks() {
        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor)executor).getCompletedTaskCount();
        } else {
            return -1;
        }
    }

    public long numPendingTasks() {
        if (executor instanceof ThreadPoolExecutor) {
            return ((ThreadPoolExecutor)executor).getQueue().size();
        } else {
            return -1;
        }
    }
}
