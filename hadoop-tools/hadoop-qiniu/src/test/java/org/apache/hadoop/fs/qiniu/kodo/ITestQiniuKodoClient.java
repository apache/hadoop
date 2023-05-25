package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoFileInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.*;

public class ITestQiniuKodoClient {
    protected IQiniuKodoClient client;

    @Before
    public void setup() throws Exception {
        client = new MockQiniuKodoClient();
    }

    public byte[] dataset(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    @Test
    public void testUploadAndFetch() throws IOException {
        MockQiniuKodoClient client = new MockQiniuKodoClient();

        // Upload a test file
        String testKey = "test_key";
        byte[] testData = dataset(1024 * 1024 * 10); // 10MB test data
        InputStream testStream = new ByteArrayInputStream(testData);
        assertTrue(client.upload(testStream, testKey, true));

        // Fetch the file and check its content
        InputStream fetchedStream = client.fetch(testKey, 0, testData.length);
        byte[] fetchedData = IOUtils.readFullyToByteArray(new DataInputStream(fetchedStream));
        assertArrayEquals(testData, fetchedData);
    }

    @Test
    public void testListStatus() throws IOException {
        MockQiniuKodoClient client = new MockQiniuKodoClient();

        // Upload some test files
        byte[] testData1 = "Test data 1".getBytes();
        InputStream testStream1 = new ByteArrayInputStream(testData1);
        assertTrue(client.upload(testStream1, "test_key1", true));

        byte[] testData2 = "Test data 2".getBytes();
        InputStream testStream2 = new ByteArrayInputStream(testData2);
        assertTrue(client.upload(testStream2, "test_key2", true));

        byte[] testData3 = "Test data 3".getBytes();
        InputStream testStream3 = new ByteArrayInputStream(testData3);
        assertTrue(client.upload(testStream3, "dir/test_key3", true));

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
        MockQiniuKodoClient client = new MockQiniuKodoClient();

        // Upload a test file
        String testKey = "test_key";
        byte[] testData = "Hello, world!".getBytes();
        InputStream testStream = new ByteArrayInputStream(testData);
        assertTrue(client.upload(testStream, testKey, true));

        // Check if the file exists
        assertTrue(client.exists(testKey));
        assertFalse(client.exists("nonexistent_key"));
    }

    @Test
    public void testDelete() throws IOException {
        MockQiniuKodoClient client = new MockQiniuKodoClient();

        // Upload a test file
        String testKey = "test_key";
        byte[] testData = "Hello, world!".getBytes();
        InputStream testStream = new ByteArrayInputStream(testData);
        assertTrue(client.upload(testStream, testKey, true));

        // Delete the file
        assertTrue(client.deleteKey(testKey));

        // Check if the file still exists
        assertFalse(client.exists(testKey));
    }

    @Test
    public void testCopy() throws IOException {
        MockQiniuKodoClient client = new MockQiniuKodoClient();

        // Upload a test file
        String testKey = "test_key";
        byte[] testData = "Hello, world!".getBytes();
        InputStream testStream = new ByteArrayInputStream(testData);
        assertTrue(client.upload(testStream, testKey, true));

        // Copy the file to a new key
        String newKey = "new_key";
        assertTrue(client.copyKey(testKey, newKey));

        // Fetch the copied file and check its content
        InputStream fetchedStream = client.fetch(newKey, 0, testData.length);
        byte[] fetchedData = IOUtils.readFullyToByteArray(new DataInputStream(fetchedStream));
        assertArrayEquals(testData, fetchedData);
    }
}
