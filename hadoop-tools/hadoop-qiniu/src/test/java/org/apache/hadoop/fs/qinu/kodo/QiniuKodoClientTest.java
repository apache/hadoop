package org.apache.hadoop.fs.qinu.kodo;

import com.qiniu.common.QiniuException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class QiniuKodoClientTest {
    QiniuKodoClient client;
    @Before
    public void setup() throws AuthorizationException, QiniuException {
        Configuration conf = new Configuration();
        conf.addResource("contract-test-options.xml");

        QiniuKodoFsConfig fsConfig = new QiniuKodoFsConfig(conf);
        String bucket = URI.create(conf.get("fs.contract.test.fs.kodo")).getHost();
        client = new QiniuKodoClient(bucket, fsConfig);
    }

    @Test
    public void testMkdir() throws IOException {
        client.makeEmptyObject("aaab/");
    }

    @Test
    public void testListStatus() throws IOException {
        client.listStatus("", false).forEach((e)->{
            System.out.println(e.key);
        });
    }

    @Test
    public void testGetFileStatus() throws IOException {
        System.out.println(client.getFileStatus("user"));
    }

    @Test
    public void testDeleteKeys() throws IOException {
        client.deleteKeys("");
    }

    @Test
    public void testFetch() throws IOException {
        int blockSize = 4*1024*1024;
        byte[] buf = new byte[blockSize];
        for (int i=0;;i++) {
            InputStream is = client.fetch("vscode2.zip", (long) blockSize *i, blockSize);
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
}
