package org.apache.hadoop.fs.qinu.kodo;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.Region;
import com.qiniu.util.Auth;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFsConfig;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class QiniuKodoClientTest {
    QiniuKodoClient client;
    @Before
    public void setup() throws AuthorizationException, QiniuException {
        Configuration conf = new Configuration();
        conf.addResource("contract-test-options.xml");

        QiniuKodoFsConfig fsConfig = new QiniuKodoFsConfig(conf);
        String bucket = URI.create(conf.get("fs.contract.test.fs.qiniu")).getHost();
        client = new QiniuKodoClient(fsConfig.createAuth(), bucket, fsConfig);
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
}
