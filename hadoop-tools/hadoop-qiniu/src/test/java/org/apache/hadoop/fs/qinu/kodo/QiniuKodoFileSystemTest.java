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
        byte[] buf = new byte[4*1024*1024];
        while((is.read(buf)) != -1) {
//            System.out.print(ch + " ");
        }
        is.close();
    }

    void writeEmptyFile(String file) throws IOException {
        fs.create(new Path(file)).close();
    }

    @Test
    public void testEmptyFileWriteRead() throws IOException {
        writeEmptyFile("emptyFile.txt");
        FSDataInputStream is = fs.open(new Path("emptyFile.txt"));
        byte[] buf = new byte[4*1024*1024];
        while((is.read(buf)) != -1) {
        }
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

    @Test
    public void testReadFully() throws IOException {
        byte[] data = new byte[256];
        for(int i=0;i<256;i++) {
            data[i] = (byte) i;
        }

        FSDataOutputStream os = fs.create(new Path("testFile.tmp"), true);
        os.write(data);
        os.close();

        FSDataInputStream is = fs.open(new Path("testFile.tmp"));
        byte[] buf = new byte[5];

        int sz;
        while((sz = is.read(buf)) != -1) {
            LOG.info("sz: {}, buf: {}", sz, buf);
        }

        is.close();
    }


    @Test
    public void testReadBigFile1() throws IOException {
        FSDataInputStream is = fs.open(new Path("/vscode.zip"));
        int ch;
        long st = System.currentTimeMillis();
        while ((ch = is.read()) != -1) {
        }
        is.close();
        long et = System.currentTimeMillis();
        System.out.printf("%f\n", (double)(et - st) / 1000f);
    }
    @Test
    public void testReadBigFile() throws Exception {
        FSDataInputStream is = fs.open(new Path("/vscode.zip"));
        byte[] buf = new byte[4*1024*1024];
        int sz;
        long st = System.currentTimeMillis();
        while ((sz = is.read(buf)) != -1) {
//            System.out.println(sz);
        }
        is.close();
        long et = System.currentTimeMillis();
        System.out.printf("%f\n", (double)(et - st) / 1000f);
    }
}
