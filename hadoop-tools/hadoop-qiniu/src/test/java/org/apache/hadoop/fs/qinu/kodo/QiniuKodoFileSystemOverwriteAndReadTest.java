package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class QiniuKodoFileSystemOverwriteAndReadTest extends FileSystemContractBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystemOverwriteAndReadTest.class);

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        fs = new QiniuKodoFileSystem();
        fs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);

    }

    @Test
    public void testOverWriteAndRead1() throws Exception {
        int success = 0;
        while (true) {
            try {
                testOverWriteAndRead();
                success++;
            } catch (Throwable e) {
                LOG.error("Success: " + success + " Error: ", e);
                break;
            }
        }
    }

}
