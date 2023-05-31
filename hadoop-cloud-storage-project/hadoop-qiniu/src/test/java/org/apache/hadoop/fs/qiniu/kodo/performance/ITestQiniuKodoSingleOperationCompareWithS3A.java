package org.apache.hadoop.fs.qiniu.kodo.performance;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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

public class ITestQiniuKodoSingleOperationCompareWithS3A {
    private final QiniuKodoFileSystem kodoFs = new QiniuKodoFileSystem();
    private final S3AFileSystem s3aFs = new S3AFileSystem();
    private static final Logger LOG = LoggerFactory.getLogger(ITestQiniuKodoSingleOperationCompareWithS3A.class);

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
        long ms = System.currentTimeMillis();
        s3aFs.listStatus(new Path("/testKodo/ListBigDirectorySeriallyTest/"));
        ms = System.currentTimeMillis() - ms;
        System.out.println("s3a: " + ms);
    }

    @Test
    public void testKodoList() throws IOException {
        long ms = System.currentTimeMillis();
        kodoFs.listStatus(new Path("/testKodo/ListBigDirectorySeriallyTest/"));
        ms = System.currentTimeMillis() - ms;
        System.out.println("s3a: " + ms);
    }


    @Test
    public void testS3AMkdirDeeply() throws Exception {
        StringBuilder sb = new StringBuilder("/s3aDeeply");
        for (int i = 0; i < 10; i++) {
            sb.append("/").append(i);
        }
        s3aFs.mkdirs(new Path(sb.toString()));
    }

    @Test
    public void testKodoMkdirDeeply() throws Exception {
        StringBuilder sb = new StringBuilder("/kodoDeeply");
        for (int i = 0; i < 10; i++) {
            sb.append("/").append(i);
        }
        kodoFs.mkdirs(new Path(sb.toString()));
    }

    @Test
    public void testS3ACreateSmallFile() throws Exception {
        s3aFs.create(new Path("/s3aSmallFile")).close();
    }

    @Test
    public void testKodoCreateSmallFile() throws Exception {
        kodoFs.create(new Path("/kodoSmallFile")).close();
    }

    private void createBigFile(FileSystem fs, Path path) throws Exception {
        long ms = System.currentTimeMillis();
        byte[] bs = new byte[40 * 1024 * 1024];
        FSDataOutputStream fso = fs.create(path);
        for (int i = 0; i < 2; i++) {
            fso.write(bs);
        }
        fso.close();
        long useMs = System.currentTimeMillis() - ms;
        LOG.info("Use time: {}", useMs);
    }

    @Test
    public void testS3ACreateBigFile() throws Exception {
        createBigFile(s3aFs, new Path("/s3aBigFile"));
    }

    @Test
    public void testKodoCreateBigFile() throws Exception {
        createBigFile(kodoFs, new Path("/kodoBigFile"));
    }
}
