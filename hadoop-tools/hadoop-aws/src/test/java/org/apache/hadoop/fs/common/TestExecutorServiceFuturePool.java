package org.apache.hadoop.fs.common;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestExecutorServiceFuturePool {
    @Test
    public void testRunnableSucceeds() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
            final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
            Future<Void> future = futurePool.apply(() -> atomicBoolean.set(true));
            future.get(30, TimeUnit.SECONDS);
            assertTrue(atomicBoolean.get());
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testSupplierSucceeds() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
            final AtomicBoolean atomicBoolean = new AtomicBoolean(false);
            Future<Void> future = futurePool.apply(() -> {
                atomicBoolean.set(true);
                return null;
            });
            future.get(30, TimeUnit.SECONDS);
            assertTrue(atomicBoolean.get());
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testRunnableFails() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
            Future<Void> future = futurePool.apply((Runnable) () -> {
                throw new IllegalStateException("deliberate");
            });
            assertThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testSupplierFails() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
            Future<Void> future = futurePool.apply(new Supplier<Void>() {
                @Override
                public Void get() {
                    throw new IllegalStateException("deliberate");
                }
            });
            assertThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
        } finally {
            executorService.shutdownNow();
        }
    }
}
