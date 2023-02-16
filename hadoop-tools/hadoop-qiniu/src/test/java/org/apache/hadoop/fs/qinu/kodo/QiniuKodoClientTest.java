package org.apache.hadoop.fs.qinu.kodo;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

public class QiniuKodoClientTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoClient.class);
    QiniuKodoClient client;

    @Before
    public void setup() throws AuthorizationException, QiniuException {
        Configuration conf = new Configuration();
        conf.addResource("contract-test-options.xml");

        QiniuKodoFsConfig fsConfig = new QiniuKodoFsConfig(conf);
        String bucket = URI.create(conf.get("fs.contract.test.fs.kodo")).getHost();
        client = new QiniuKodoClient(bucket, fsConfig, null);
    }

    @Test
    public void testMkdir() throws IOException {
        client.makeEmptyObject("aaab/");
    }

    @Test
    public void testListStatus() throws IOException {
        client.listStatus("", false).forEach((e) -> {
            System.out.println(e.key);
        });
    }

    @Test
    public void testGetFileStatus() throws IOException {
        System.out.println(client.getFileStatus("user"));
    }

    @Test
    public void testDeleteKeys() throws IOException {
        client.deleteKeys("", true);
    }

    @Test
    public void testFetch() throws IOException {
        int blockSize = 4 * 1024 * 1024;
        byte[] buf = new byte[blockSize];
        for (int i = 0; ; i++) {
            InputStream is = client.fetch("vscode2.zip", (long) blockSize * i, blockSize);
            int total = 0;
            int sz = 0;
            while ((sz = is.read(buf)) != -1) {
                total += sz;
            }
            if (total < blockSize) {
                break;
            }
        }

    }

    @Test
    public void testListStatus12Equals() throws Exception {
        // jit
        client.listStatus("testKodo/ListBigDirectorySeriallyTest/", false);
        client.listStatusOld("testKodo/ListBigDirectorySeriallyTest/", false);

        long useTime, ms;
        ms = System.currentTimeMillis();
        List<FileInfo> fileInfoList1 = client.listStatus("testKodo/ListBigDirectorySeriallyTest/", false);
        useTime = System.currentTimeMillis() - ms;
        LOG.info("Use time: {}, size: {}", useTime, fileInfoList1.size());

        ms = System.currentTimeMillis();
        List<FileInfo> fileInfoList2 = client.listStatusOld("testKodo/ListBigDirectorySeriallyTest/", false);
        useTime = System.currentTimeMillis() - ms;
        LOG.info("Use time: {}, size: {}", useTime, fileInfoList2.size());
        for (int i = 0; i < 10000; i++) {
            Assert.assertEquals(
                    fileInfoList1.get(i).key,
                    fileInfoList2.get(i).key
            );
        }
    }

    @Test
    public void testList() throws Exception {
        BucketManager bm = client.bucketManager;
        FileListing fl = bm.listFiles("qshell-hadoop", "testKodo/ListBigDirectorySeriallyTest/", null, 1, "");
        System.out.println(fl);
    }

    @Test
    public void testCopyKeysTime1() throws Exception {
        long useTime, ms;
//        ms = System.currentTimeMillis();
//        client.copyKeys1("testKodo/ListBigDirectorySeriallyTest/", "testClient/copyKey1/");
//        useTime = System.currentTimeMillis() - ms;
//        LOG.info("Use time: {}", useTime);

        ms = System.currentTimeMillis();
        client.copyKeys("testKodo/ListBigDirectorySeriallyTest/", "testClient/copyKey2/");
        useTime = System.currentTimeMillis() - ms;
        LOG.info("Use time: {}", useTime);
    }

    @Test
    public void testCopyAndCount() throws Exception {
        long useTime, ms;
        String oldPrefix = "testKodo/ListBigDirectorySeriallyTest/";
        String newPrefix = "testClient/copyKey123/";

        ms = System.currentTimeMillis();
        int s1 = client.listStatus(oldPrefix, false).size();
        useTime = System.currentTimeMillis() - ms;
        LOG.info("Use time: {}", useTime);

        client.copyKeys(oldPrefix, newPrefix);

        ms = System.currentTimeMillis();
        client.copyKeys(oldPrefix, newPrefix);
        useTime = System.currentTimeMillis() - ms;
        LOG.info("Use time: {}", useTime);

        ms = System.currentTimeMillis();
        int s2 = client.listStatus(newPrefix, false).size();
        useTime = System.currentTimeMillis() - ms;
        LOG.info("Use time: {}", useTime);

        Assert.assertEquals(s1, s2);
    }

}
