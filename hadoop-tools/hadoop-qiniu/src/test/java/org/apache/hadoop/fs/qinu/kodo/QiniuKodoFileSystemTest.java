package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.qiniu.kodo.QiniuKodoFileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class QiniuKodoFileSystemTest {
    private static final Logger LOG = LoggerFactory.getLogger(QiniuKodoFileSystemTest.class);
    private FileSystem fs;

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("contract-test-options.xml");

        fs = new QiniuKodoFileSystem();
        fs.initialize(URI.create(conf.get("fs.contract.test.fs.kodo")), conf);
    }

    @After
    public void exit() throws Exception {
        if (fs != null) {
            fs.close();
        }
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
        Path oldPath = new Path("/abc");
        Path newPath = new Path("/abc_new");

        fs.create(oldPath).close();
        assertTrue(fs.exists(oldPath));
        assertFalse(fs.exists(newPath));

        fs.rename(oldPath, newPath);
        assertFalse(fs.exists(oldPath));
        assertTrue(fs.exists(newPath));
    }

    @Test
    public void testEmptyFileWriteRead() throws IOException {
        fs.create(new Path("emptyFile.txt")).close();
        FSDataInputStream is = fs.open(new Path("emptyFile.txt"));
        assertEquals(-1, is.read());
        is.close();
    }

    @Test
    public void testWriteAndRead() throws IOException {
        Path file = new Path("writeFile.txt");
        FSDataOutputStream os = fs.create(file);
        for (int i = 0; i < 100; i++) {
            os.write(i);
        }
        os.close();
        FSDataInputStream is = fs.open(file);
        for (int i = 0; i < 100; i++) {
            assertEquals(i, is.read());
        }
        assertEquals(-1, is.read());
        is.close();
    }

    @Test
    public void testListSmallFiles() throws Exception {
        Path dir = new Path("testSmallFiles");

        Set<Path> ps = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            Path file = new Path(dir.toString(), i + ".txt").makeQualified(fs.getUri(), fs.getWorkingDirectory());
            fs.create(file).close();
            assertTrue(fs.exists(file));
            ps.add(file);
        }
        FileStatus[] fss = fs.listStatus(dir);
        assertEquals(100, fss.length);
        Set<Path> fileStatusSet = Arrays.stream(fss)
                .map(FileStatus::getPath)
                .collect(Collectors.toSet());
        assertEquals(ps, fileStatusSet);
    }

    @Test
    public void testWriteBigFile() throws IOException {
        byte[] bs = new byte[40 * 1024 * 1024];
        FSDataOutputStream os = fs.create(new Path("/bigFile.txt"));
        for (int i = 0; i < 3; i++) {
            LOG.info("write block: {}", i);
            os.write(bs);
        }
        os.close();
    }
}
