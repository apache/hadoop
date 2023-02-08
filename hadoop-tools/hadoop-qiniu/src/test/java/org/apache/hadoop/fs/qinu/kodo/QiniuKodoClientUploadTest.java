package org.apache.hadoop.fs.qinu.kodo;

import com.qiniu.http.Response;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class QiniuKodoClientUploadTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoClientUploadTest.class);

    // 构造数据集
    byte[] dataset(int len, int base, int modulo) {
        byte[] dataset = new byte[len];
        for (int i = 0; i < len; i++) {
            dataset[i] = (byte) (base + (i % modulo));
        }
        return dataset;
    }

    void testWriteAndRead(QiniuKodoClient client, byte[] data) throws Exception {
        String key = "file-overwrite";
        Response uploadResponse = client.upload(new ByteArrayInputStream(data), key, true);

        Assert.assertTrue(uploadResponse.isOK());

        byte[] receiveData = new byte[data.length];
        try (InputStream is = client.fetch(key, 0, data.length)) {
            IOUtils.readFully(is, receiveData);
        }
        for (int i = 0; i < data.length; i++) {
            Assert.assertEquals(data[i], receiveData[i]);
        }
    }

    private QiniuKodoClient client;

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        QiniuKodoFsConfig fsConfig = new QiniuKodoFsConfig(conf);
        this.client = new QiniuKodoClient("qshell-hadoop", fsConfig, null);
    }

    @Test
    public void test() throws Exception {
        int blockSize = 4096;
        byte[] filedata1 = dataset(blockSize, 'A', 26);
        byte[] filedata2 = dataset(blockSize, 'a', 26);

        for (int i = 0; i < 10; i++) {
            LOG.info("Count: {}", i);
            testWriteAndRead(client, filedata1);
            testWriteAndRead(client, filedata2);
        }
    }
}
