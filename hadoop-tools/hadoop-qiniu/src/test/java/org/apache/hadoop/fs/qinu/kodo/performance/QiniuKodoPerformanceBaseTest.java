package org.apache.hadoop.fs.qinu.kodo.performance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoPerformanceBaseTest.class);
    private static final String DEFAULT_KODO_TEST_DIR = "/testKodo";
    private static final String DEFAULT_S3A_TEST_DIR = "/testS3A";
    private final FileSystem kodoFs = new QiniuKodoFileSystem();
    private final FileSystem s3aFs = new S3AFileSystem();

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        kodoFs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
        s3aFs.initialize(URI.create(conf.get("fs.contract.test.fs.s3a")), conf);
    }

    /**
     * 构造测试任务的执行器
     */
    abstract protected ExecutorService buildExecutorService();

    // 需要用户实现该方法
    abstract protected long testImpl(String testDir, FileSystem fs, ExecutorService executorService) throws Exception;

    protected long testS3AImpl(String testDir, FileSystem fs) throws Exception {
        return testImpl(testDir, fs, buildExecutorService());
    }

    protected long testKodoImpl(String testDir, FileSystem fs) throws Exception {
        return testImpl(testDir, fs, buildExecutorService());
    }

    protected long timeoutN() {
        return 1;
    }

    protected TimeUnit timeoutUnit() {
        return TimeUnit.MINUTES;
    }

    protected void awaitAllExecutors(ExecutorService service) throws InterruptedException, TimeoutException {
        long n = timeoutN();
        TimeUnit unit = timeoutUnit();

        service.shutdown();
        // 规定时间内没结束
        if (!service.awaitTermination(n, unit)) {
            throw new TimeoutException(String.format("timeout: %d, %s", n, unit));
        }
    }

    protected String getKodoTestDir() {
        return DEFAULT_KODO_TEST_DIR;
    }

    protected String getS3ATestDir() {
        return DEFAULT_S3A_TEST_DIR;
    }

    protected String getSceneString() {
        return this.getClass().getSimpleName();
    }

    // 构造该测试场景的文件夹名称
    protected String getSceneWorkDirName() {
        return getSceneString();
    }

    @Test
    public void testS3A() throws Exception {
        long time = testS3AImpl(String.format("/%s/%s", getS3ATestDir(), getSceneWorkDirName()), s3aFs);
        LOG.info(getSceneString() + " (S3A) " + "cost time: " + time);
    }

    @Test
    public void testKodo() throws Exception {
        long time = testKodoImpl(String.format("/%s/%s", getKodoTestDir(), getSceneWorkDirName()), kodoFs);
        LOG.info(getSceneString() + " (Kodo) " + "cost time: " + time);
    }
}
