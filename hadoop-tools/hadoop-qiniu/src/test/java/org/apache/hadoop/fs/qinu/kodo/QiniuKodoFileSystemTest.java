package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.qiniu.kodo.Constants;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.apache.hadoop.fs.qinu.kodo.contract.QiniuKodoContract;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

public class QiniuKodoFileSystemTest {
    private FileSystem fs;

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

    @Test
    public void testMKDir() throws Exception {
        boolean success = fs.mkdirs(new Path("01/02/03/04/05"), null);
        assertTrue(success);
    }

    @Test
    public void getDirStat() throws Exception {
        FileStatus fileStatus = fs.getFileStatus(new Path("01/02/03/04/05"));
        assertNotNull(fileStatus);
    }

    @Test
    public void listDir() throws Exception {
        FileStatus[] fileStatusList = fs.listStatus(new Path("01/02/03/04"));
        for (FileStatus file : fileStatusList) {
            System.out.println(file.toString());
        }
        assertNotNull(fileStatusList);
        assertTrue(fileStatusList.length > 0);
    }
}
