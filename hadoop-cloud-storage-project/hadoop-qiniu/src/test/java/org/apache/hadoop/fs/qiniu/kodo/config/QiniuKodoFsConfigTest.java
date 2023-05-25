package org.apache.hadoop.fs.qiniu.kodo.config;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class QiniuKodoFsConfigTest {
    @Test
    public void testLoad() throws Exception {
        String testAccessKeyStr = "test123";
        String testSecretKeyStr = "test234";
        Configuration conf = new Configuration();
        conf.set("fs.qiniu.auth.accessKey", testAccessKeyStr);
        conf.set("fs.qiniu.auth.secretKey", testSecretKeyStr);
        QiniuKodoFsConfig fsConfig = new QiniuKodoFsConfig(conf);
        Assert.assertEquals(testAccessKeyStr, fsConfig.auth.accessKey);
        Assert.assertEquals(testSecretKeyStr, fsConfig.auth.secretKey);
    }
}
