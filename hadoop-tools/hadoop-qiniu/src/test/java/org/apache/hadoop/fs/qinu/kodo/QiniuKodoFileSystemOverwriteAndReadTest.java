package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class QiniuKodoFileSystemOverwriteAndReadTest extends FileSystemContractBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystemOverwriteAndReadTest.class);

    private void testOverWriteAndReadBy(FileSystem fileSystem) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        fs = fileSystem;
        fs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);

        int success = 0;
        while (true) {
            try {
                LOG.info("Count: {}", success);
                testOverWriteAndRead();
                success++;
            } catch (Throwable e) {
                LOG.error("Success: " + success + " Error: ", e);
                throw e;
            }
        }
    }

    @Test
    public void testOverWriteAndReadByKodo() throws Exception {
        testOverWriteAndReadBy(new QiniuKodoFileSystem());
    }

    @Test
    public void testOverWriteAndReadByS3A() throws Exception {
        testOverWriteAndReadBy(new S3AFileSystem());
    }

    @Override
    protected int getGlobalTimeout() {
        return Integer.MAX_VALUE;
    }
}
