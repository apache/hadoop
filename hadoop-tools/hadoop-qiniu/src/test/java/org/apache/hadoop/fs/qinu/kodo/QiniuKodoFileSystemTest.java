package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.qiniu.kodo.Constants;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.apache.hadoop.fs.qinu.kodo.contract.QiniuKodoContract;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

public class QiniuKodoFileSystemTest {
    private QiniuKodoFileSystem createFileSystem() throws Exception {
        QiniuKodoFileSystem fs = new QiniuKodoFileSystem();
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");
        Map<String, String> env = System.getenv();
        conf.setIfUnset(Constants.QINIU_PARAMETER_ACCESS_KEY, env.get("QSHELL_AK"));
        conf.setIfUnset(Constants.QINIU_PARAMETER_SECRET_KEY, env.get("QSHELL_SK"));
        String testUri = conf.get("fs.contract.test.fs.qiniu");
        fs.initialize(URI.create(testUri != null ? testUri : "qiniu://qshell-hadoop"), conf);
        return fs;
    }

    @Test
    public void testMKDir() throws Exception {
        QiniuKodoFileSystem fs = createFileSystem();
        boolean success = fs.mkdirs(new Path("01/02/03/04/05"), null);
        Assert.assertTrue(success);
    }

    @Test
    public void getDirStat() throws Exception {
        QiniuKodoFileSystem fs = createFileSystem();
        FileStatus fileStatus = fs.getFileStatus(new Path("01/02/03/04/05"));
        Assert.assertNotNull(fileStatus);
    }

    @Test
    public void listDir() throws Exception {
        QiniuKodoFileSystem fs = createFileSystem();
        FileStatus[] fileStatusList = fs.listStatus(new Path("01/02/03/04/05"));
        for (FileStatus file : fileStatusList) {
            System.out.println(file.toString());
        }
        Assert.assertNotNull(fileStatusList);
        Assert.assertTrue(fileStatusList.length > 0);
    }
}
