package org.apache.hadoop.fs.qinu.kodo.performance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class QiniuKodoSingleOperationCompareWithS3ATest {
    private final FileSystem kodoFs = new QiniuKodoFileSystem();
    private final FileSystem s3aFs = new S3AFileSystem();
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoSingleOperationCompareWithS3ATest.class);

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        kodoFs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
        s3aFs.initialize(URI.create(conf.get("fs.contract.test.fs.s3a")), conf);
    }

    @Test
    public void testS3AList() throws IOException {
        s3aFs.listStatus(new Path("/testS3A/CreateSmallFileConcurrentlyTest/"));
    }

    @Test
    public void testKodoList() throws IOException {
        kodoFs.listStatus(new Path("/testS3A/CreateSmallFileConcurrentlyTest/"));
    }

}