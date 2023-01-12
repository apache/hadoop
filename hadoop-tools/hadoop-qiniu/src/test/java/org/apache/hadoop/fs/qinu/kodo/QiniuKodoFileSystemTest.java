package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class QiniuKodoFileSystemTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystemTest.class);
    private FileSystem fs;

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        conf.setIfUnset("fs.contract.test.fs.kodo", "kodo://qshell-hadoop");

        fs = new QiniuKodoFileSystem();
        fs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
    }

    @Test
    public void testMkdir() throws Exception {
        assertTrue(fs.mkdirs(new Path("01/02/03/04/05")));
        assertTrue(fs.mkdirs(new Path(fs.getWorkingDirectory(), "01/02/03/04/05")));
    }

    @Test
    public void getDirStat() throws Exception {
        Path path = new Path("01/02/03/04/05");
        fs.mkdirs(path);
        FileStatus fileStatus = fs.getFileStatus(path);
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
    public void testOpen() throws IOException, InterruptedException {
        FSDataInputStream is = fs.open(new Path("/vscode2.zip"));
        int ch;
        int cnt = 0;
        while((ch = is.read()) != -1) {
//            System.out.print(ch + " ");
            cnt++;
        }
        LOG.info("cnt: {}", cnt);
        is.close();
    }

    @Test
    public void testWrite() throws IOException {
        FSDataOutputStream os = fs.create(new Path("/user/zzq123.txt"));
        for (int i=0;i<100;i++) {
            os.write(i);
        }
        os.close();
    }
}
