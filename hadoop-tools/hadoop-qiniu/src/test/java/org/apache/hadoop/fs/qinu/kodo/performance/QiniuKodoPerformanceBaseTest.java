package org.apache.hadoop.fs.qinu.kodo.performance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.Before;

import java.net.URI;

public class QiniuKodoPerformanceBaseTest {

    protected FileSystem kodo = new QiniuKodoFileSystem();
    protected FileSystem s3a = new S3AFileSystem();
    protected String kodoTestDir = "/testKodo";
    protected String s3aTestDir = "/testS3A";

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        kodo.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
        s3a.initialize(URI.create(conf.get("fs.contract.test.fs.s3a")), conf);
    }
}
