package org.apache.hadoop.fs.qinu.kodo;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.Region;
import com.qiniu.util.Auth;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.Constants;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class QiniuKodoClientTest {
    QiniuKodoClient client;
    @Before
    public void setup() throws QiniuException {
        Configuration conf = new Configuration();
        conf.addResource("contract-test-options.xml");

        com.qiniu.storage.Configuration qiniuConfig = new com.qiniu.storage.Configuration();
        qiniuConfig.region = Region.autoRegion();

        String accessKey = conf.get(Constants.QINIU_PARAMETER_ACCESS_KEY);
        String secretKey = conf.get(Constants.QINIU_PARAMETER_SECRET_KEY);
        Auth auth = Auth.create(accessKey, secretKey);
        String bucket = URI.create(conf.get("fs.contract.test.fs.qiniu")).getHost();
        client = new QiniuKodoClient(auth, qiniuConfig, bucket);
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
