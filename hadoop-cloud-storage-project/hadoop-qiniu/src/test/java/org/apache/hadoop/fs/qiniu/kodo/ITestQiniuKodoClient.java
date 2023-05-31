package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoFileInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.*;

public class ITestQiniuKodoClient {
    protected IQiniuKodoClient client;

    @Before
    public void setup() throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(TestConstants.FILE_CORE_SITE_XML);
        conf.addResource(TestConstants.FILE_CONTRACT_TEST_OPTIONS_XML);

        if (conf.getBoolean(
                TestConstants.CONFIG_TEST_USE_MOCK_KEY,
                TestConstants.CONFIG_TEST_USE_MOCK_DEFAULT_VALUE
        )) {
            client = new MockQiniuKodoClient();
        } else {
            try (QiniuKodoFileSystem fs = new QiniuKodoFileSystem()) {
                fs.initialize(URI.create(conf.get(TestConstants.CONFIG_TEST_CONTRACT_FS_KEY)), conf);
                fs.delete(new Path("/"), true);
                client = fs.getKodoClient();
            }
        }
    }

    public byte[] dataset(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (Math.random() * 256);
        }
        return data;
    }

    @Test
    public void testUploadOverWrite() throws IOException {
        byte[] data1 = dataset(1024);
        client.upload(new ByteArrayInputStream(data1), "test_key", true);
        byte[] data2 = dataset(1024);
        client.upload(new ByteArrayInputStream(data2), "test_key", true);
    }

    @Test
    public void testUploadAndFetch() throws IOException {
        // Upload a test file
        String testKey = "test_key";
        byte[] testData = dataset(1024 * 1024 * 10); // 10MB test data
        InputStream testStream = new ByteArrayInputStream(testData);
        client.upload(testStream, testKey, true);

        // Fetch the file and check its content
        InputStream fetchedStream = client.fetch(testKey, 0, testData.length);
        byte[] fetchedData = IOUtils.readFullyToByteArray(new DataInputStream(fetchedStream));
        assertArrayEquals(testData, fetchedData);
    }

    @Test
    public void testListStatus() throws IOException {
        // Upload some test files
        byte[] testData1 = "Test data 1".getBytes();
        InputStream testStream1 = new ByteArrayInputStream(testData1);
        client.upload(testStream1, "test_key1", true);

        byte[] testData2 = "Test data 2".getBytes();
        InputStream testStream2 = new ByteArrayInputStream(testData2);
        client.upload(testStream2, "test_key2", true);

        byte[] testData3 = "Test data 3".getBytes();
        InputStream testStream3 = new ByteArrayInputStream(testData3);
        client.upload(testStream3, "dir/test_key3", true);

        // List all files
        List<QiniuKodoFileInfo> allFiles = client.listStatus("", false);
        assertEquals(3, allFiles.size());

        // List files with prefix "test_"
        List<QiniuKodoFileInfo> testFiles = client.listStatus("test_", false);
        assertEquals(2, testFiles.size());

        // List files with prefix "dir/"
        List<QiniuKodoFileInfo> dirFiles = client.listStatus("dir/", false);
        assertEquals(1, dirFiles.size());

        // List all files, including directories
        List<QiniuKodoFileInfo> allFilesAndDirs = client.listStatus("", true);
        assertEquals(3, allFilesAndDirs.size());
    }

    @Test
    public void testExists() throws IOException {
        // Upload a test file
        String testKey = "test_key";
        byte[] testData = "Hello, world!".getBytes();
        InputStream testStream = new ByteArrayInputStream(testData);
        client.upload(testStream, testKey, true);

        // Check if the file exists
        assertTrue(client.exists(testKey));
        assertFalse(client.exists("nonexistent_key"));
    }

    @Test
    public void testDelete() throws IOException {
        // Upload a test file
        String testKey = "test_key";
        byte[] testData = "Hello, world!".getBytes();
        InputStream testStream = new ByteArrayInputStream(testData);
        client.upload(testStream, testKey, true);

        // Delete the file
        client.deleteKey(testKey);

        // Check if the file still exists
        assertFalse(client.exists(testKey));
    }

    @Test
    public void testCopy() throws IOException {
        // Upload a test file
        String testKey = "test_key";
        byte[] testData = "Hello, world!".getBytes();
        InputStream testStream = new ByteArrayInputStream(testData);
        client.upload(testStream, testKey, true);

        // Copy the file to a new key
        String newKey = "new_key";
        client.copyKey(testKey, newKey);

        // Fetch the copied file and check its content
        InputStream fetchedStream = client.fetch(newKey, 0, testData.length);
        byte[] fetchedData = IOUtils.readFullyToByteArray(new DataInputStream(fetchedStream));
        assertArrayEquals(testData, fetchedData);
    }

    @Test
    public void testMakeEmptyObject() throws IOException {
        String testKey = "test_key";
        client.makeEmptyObject(testKey);
        QiniuKodoFileInfo fileInfo = client.getFileStatus(testKey);
        assertEquals(0, fileInfo.size);
        assertEquals(testKey, fileInfo.key);
    }
}
