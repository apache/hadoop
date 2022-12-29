package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.Constants;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.Map;

public class QiniuKodoFileSystemTest {

    private static QiniuKodoFileSystem createFileSystem() throws Exception {
        QiniuKodoFileSystem fs = new QiniuKodoFileSystem();
        Configuration configuration = new Configuration();
        Map<String, String> env = System.getenv();
        configuration.set(Constants.QINIU_PARAMETER_ACCESS_KEY, env.get("QSHELL_AK"));
        configuration.set(Constants.QINIU_PARAMETER_SECRET_KEY, env.get("QSHELL_SK"));
        fs.initialize(new URI("qiniu://qshell-hadoop"), configuration);
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
