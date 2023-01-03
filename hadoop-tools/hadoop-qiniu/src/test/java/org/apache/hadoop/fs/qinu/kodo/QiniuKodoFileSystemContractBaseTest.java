package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.Constants;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class QiniuKodoFileSystemContractBaseTest extends FileSystemContractBaseTest {
    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        Map<String, String> env = System.getenv();
        conf.setIfUnset(Constants.QINIU_PARAMETER_ACCESS_KEY, env.get("QSHELL_AK"));
        conf.setIfUnset(Constants.QINIU_PARAMETER_SECRET_KEY, env.get("QSHELL_SK"));
        conf.setIfUnset("fs.contract.test.fs.qiniu", "qiniu://qshell-hadoop");

        fs = new QiniuKodoFileSystem();
        fs.initialize(URI.create(conf.get("fs.contract.test.fs.qiniu")), conf);

    }

    @Override
    protected int getGlobalTimeout() {
        return Integer.MAX_VALUE;
    }
}
