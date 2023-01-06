package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class QiniuKodoFileSystemTest {
    private FileSystem fs;

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        conf.setIfUnset("fs.contract.test.fs.qiniu", "kodo://qshell-hadoop");

        fs = new QiniuKodoFileSystem();
        fs.initialize(URI.create(conf.get("fs.contract.test.fs.qiniu")), conf);
    }

    @Test
    public void testMkdir() throws Exception {
        assertTrue(fs.mkdirs(new Path("01/02/03/04/05")));
        assertTrue(fs.mkdirs(new Path(fs.getWorkingDirectory(), "01/02/03/04/05")));
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

    @Test
    public void testRename() throws IOException {
        FSDataOutputStream is = fs.create(new Path("/abc"));
        is.write(12);
        is.close();
        fs.rename(new Path("/abc"), new Path("/abc_new"));
    }

    @Test
    public void testOpen() throws IOException {
        FSDataInputStream is = fs.open(new Path("/abcd"));
        int ch;
        while((ch = is.read()) != -1) {
            System.out.print(ch + " ");
        }
        is.close();
    }

    @Test
    public void testWrite() throws IOException {
        FSDataOutputStream os = fs.create(new Path("/zzq123"));
        String s = "HelloWorld Qiniu";
        os.writeUTF(s);
        os.close();
    }
}
