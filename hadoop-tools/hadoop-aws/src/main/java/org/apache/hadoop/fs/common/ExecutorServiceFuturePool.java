package org.apache.hadoop.fs.common;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

public class ExecutorServiceFuturePool {
    private ExecutorService executor;
    private boolean interruptible = false;

    public ExecutorServiceFuturePool(ExecutorService executor, boolean interruptible) {
        this.executor = executor;
    }

    public ExecutorServiceFuturePool(ExecutorService executor) {
        this(executor, false);
    }

    public Future<Void> apply(final Supplier<Void> f) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        executor.submit(() -> completableFuture.complete(f.get()));
        return completableFuture;
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
