package org.apache.hadoop.fs.qinu.kodo.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.qiniu.kodo.config.QiniuKodoFsConfig;
import org.junit.Test;

public class QiniuKodoFsConfigTest {
    @Test
    public void testLoad() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");
        QiniuKodoFsConfig fsConfig = new QiniuKodoFsConfig(conf);
        System.out.println(fsConfig);
    }
}
