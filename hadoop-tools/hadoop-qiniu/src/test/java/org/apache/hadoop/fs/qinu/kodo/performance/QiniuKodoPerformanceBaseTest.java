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

public abstract class QiniuKodoPerformanceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoPerformanceBaseTest.class);
    private final FileSystem kodo = new QiniuKodoFileSystem();
    private final FileSystem s3a = new S3AFileSystem();
    private final String kodoTestDir = "/testKodo";
    private final String s3aTestDir = "/testS3A";

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        kodo.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
        s3a.initialize(URI.create(conf.get("fs.contract.test.fs.s3a")), conf);
    }

    abstract protected long testImpl(String testDir, FileSystem fs) throws Exception;

    protected long testS3AImpl(String testDir, FileSystem fs) throws Exception {
        return testImpl(testDir, fs);
    }

    protected long testKodoImpl(String testDir, FileSystem fs) throws Exception {
        return testImpl(testDir, fs);
    }

    protected String getSceneString() {
        return this.getClass().getSimpleName();
    }

    @Test
    public void testS3A() throws Exception {
        long time = testS3AImpl(s3aTestDir, s3a);
        LOG.info(getSceneString() + " (S3A) " + "cost time: " + time);
    }

    @Test
    public void testKodo() throws Exception {
        long time = testKodoImpl(kodoTestDir, kodo);
        LOG.info(getSceneString() + " (Kodo) " + "cost time: " + time);
    }
}
