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
import java.util.concurrent.Executors;

public abstract class QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoPerformanceBaseTest.class);
    private static final String DEFAULT_KODO_TEST_DIR = "/testKodo";
    private static final String DEFAULT_S3A_TEST_DIR = "/testS3A";
    private final FileSystem kodoFs = new QiniuKodoFileSystem();
    private final FileSystem s3aFs = new S3AFileSystem();
    protected final ExecutorService service = Executors.newCachedThreadPool();

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        kodoFs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
        s3aFs.initialize(URI.create(conf.get("fs.contract.test.fs.s3a")), conf);
    }

    abstract protected long testImpl(String testDir, FileSystem fs) throws Exception;

    protected long testS3AImpl(String testDir, FileSystem fs) throws Exception {
        return testImpl(testDir, fs);
    }

    protected long testKodoImpl(String testDir, FileSystem fs) throws Exception {
        return testImpl(testDir, fs);
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

    @Test
    public void testS3A() throws Exception {
        long time = testS3AImpl(getS3ATestDir(), s3aFs);
        LOG.info(getSceneString() + " (S3A) " + "cost time: " + time);
    }

    @Test
    public void testKodo() throws Exception {
        long time = testKodoImpl(getKodoTestDir(), kodoFs);
        LOG.info(getSceneString() + " (Kodo) " + "cost time: " + time);
    }
}
