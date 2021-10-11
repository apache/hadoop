package org.apache.hadoop.yarn.health;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class HealthCheckService extends AbstractService {
    private static final Logger LOG =
        LoggerFactory.getLogger(HealthCheckService.class);

    private final Map<String, Object> reporters;
    private Thread healthCheckThread;
    private volatile boolean stopped;
    private volatile long autoCheckPeriod;
    private volatile long maxTorranceTime;
    private ReadWriteLock rwLock;
    private volatile List<HealthReport> lastReports;
    private volatile long lastTimestamp;
    private Semaphore requestSemaphore;
    private AtomicBoolean waiting;

    /**
     * Construct the service.
     *
     */
    public HealthCheckService() {
        super(HealthCheckService.class.getName());
        reporters = new ConcurrentHashMap<>();
    }

    @Override
    public void serviceInit(Configuration conf) throws Exception {
        rwLock = new ReentrantReadWriteLock();
        requestSemaphore = new Semaphore(0);
        waiting = new AtomicBoolean();
        autoCheckPeriod = conf.getLong(YarnConfiguration.HEALTH_AUTO_CHECK_PERIOD,
                YarnConfiguration.DEFAULT_HEALTH_AUTO_CHECK_PERIOD);
        maxTorranceTime = conf.getLong(YarnConfiguration.HEALTH_MAX_TORRANCE_TIME,
                YarnConfiguration.DEFAULT_HEALTH_MAX_TORRANCE_TIME);
        super.serviceInit(conf);
    }

    @Override
    public void serviceStart() throws Exception {
        stopped = false;
        healthCheckThread = new Thread(() -> {
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                try {
                    requestSemaphore.tryAcquire(autoCheckPeriod, TimeUnit.SECONDS);
                    waiting.set(true);
                    List<HealthReport> reports = innerHealthCheck();
                    requestSemaphore.drainPermits();
                    long timestamp = System.currentTimeMillis();
                    try {
                        rwLock.writeLock().lock();
                        lastReports = reports;
                        lastTimestamp = timestamp;
                    } finally {
                        rwLock.writeLock().unlock();
                        waiting.set(false);
                    }
                    // TODO log and metrics
                    for (HealthReport report : reports) {
                        LOG.info(report.toString());
                    }
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        healthCheckThread.setName("HealthCheckService Thread");
        healthCheckThread.start();
        requestSemaphore.release();
        super.serviceStart();
    }

    @Override
    public void serviceStop() throws Exception {
        stopped = true;
        if (healthCheckThread != null) {
            healthCheckThread.interrupt();
            try {
                healthCheckThread.join();
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted Exception while stopping", ie);
            }
        }
        super.serviceStop();
    }

    public List<HealthReport> innerHealthCheck() {
        List<HealthReport> results = new ArrayList<>();
        Queue<Object> candidates = new LinkedBlockingQueue<>(reporters.values());
        for (String key : reporters.keySet()) {
            LOG.info("DEBUG health check: " + key);
        }
        while (!candidates.isEmpty()) {
            Object s = candidates.poll();
            if (s == null) {
                continue;
            }
            if (s instanceof CompositeService) {
                candidates.addAll(((CompositeService) s).getServices());
            }
            if (s instanceof HealthReporter) {
                try {
                    results.add(((HealthReporter) s).getHealthReport());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return results;
    }

    public void registerReporter(String name, Object reporter) {
        if (reporters.containsKey(name)) {
            LOG.info("Replacing reporter {} with a new one", name);
        }
        reporters.put(name, reporter);
    }

    public List<HealthReport> healthCheck() {
        long now = System.currentTimeMillis();
        try {
            rwLock.readLock().lock();
            if (lastReports != null && lastTimestamp + maxTorranceTime * 1000 >= now) {
                return lastReports;
            }
        } finally {
            rwLock.readLock().unlock();
        }
        requestSemaphore.release();
        while (true) {
            while (waiting.get()) {
            }
            try {
                rwLock.readLock().lock();
                if (lastReports != null && lastTimestamp + maxTorranceTime * 1000 >= now) {
                    return lastReports;
                }
            } finally {
                rwLock.readLock().unlock();
            }
        }
    }

    public long getAutoCheckPeriod() {
        return autoCheckPeriod;
    }

    public void setAutoCheckPeriod(long autoCheckPeriod) {
        this.autoCheckPeriod = autoCheckPeriod;
    }

    public long getMaxTorranceTime() {
        return maxTorranceTime;
    }

    public void setMaxTorranceTime(long maxTorranceTime) {
        this.maxTorranceTime = maxTorranceTime;
    }

    public void refresh() {
        this.requestSemaphore.release();
    }

    public static HealthReport checkRPCServer(Server server) {
        RpcMetrics rpcMetrics = server.getRpcMetrics();
        int callQueueLength = rpcMetrics.callQueueLength();
        double queueAvgTime = rpcMetrics.getRpcQueueTime().lastStat().mean();
        double processingAvgTime = rpcMetrics.getProcessingMean();
        double estimatedDelayTime = queueAvgTime + processingAvgTime;
        Map<String, Long> threadCounts = server.getHandlerStateCount();
        double handleUtilizationRatio = (threadCounts.getOrDefault(Thread.State.RUNNABLE.toString(), 0L)
                + threadCounts.getOrDefault(Thread.State.RUNNABLE.toString(), 0L)) /
                threadCounts.values().stream().mapToDouble(x -> x).sum();
        // TODO: 需要考虑metrics存在单位转换，所以不一定是默认的ms
        HealthReport report = HealthReport.getInstance(server.getServerName(), server.isRunning(),
                estimatedDelayTime < 2000 ? HealthReport.WorkState.IDLE :
                        estimatedDelayTime > 10000 ? HealthReport.WorkState.BUSY : HealthReport.WorkState.NORMAL);
        report.putMetrics("callQueueLength", callQueueLength);
        report.putMetrics("queueAvgTime", queueAvgTime);
        report.putMetrics("processingAvgTime", processingAvgTime);
        report.putMetrics("estimatedDelayTime", estimatedDelayTime);
        report.putMetrics("handleUtilizationRatio", handleUtilizationRatio);
        for (Map.Entry<String, Long> entry : threadCounts.entrySet()) {
            report.putMetrics(entry.getKey().toLowerCase() + "HandlerThreads", entry.getValue());
        }
        return report;
    }
}
