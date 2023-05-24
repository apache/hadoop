package org.apache.hadoop.fs.qinu.kodo;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.client.MyFileInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockQiniuKodoClient implements IQiniuKodoClient {
    public static class MockFile {
        public final String key;
        public final long putTime;
        public final byte[] data;

        public MockFile(String key, long putTime, byte[] data) {
            this.key = key;
            this.putTime = putTime;
            this.data = data;
        }

        public MyFileInfo toMyFileInfo() {
            return new MyFileInfo(key, data.length, putTime);
        }

        public InputStream toInputStream(long offset, int size) {
            return new ByteArrayInputStream(data, (int) offset, size);
        }
    }

    public static Logger LOG = LoggerFactory.getLogger(MockQiniuKodoClient.class);
    public final ConcurrentHashMap<String, MockFile> mockFileMap = new ConcurrentHashMap<>();

    @Override
    public boolean exists(String key) throws IOException {
        return mockFileMap.containsKey(key);
    }

    @Override
    public long getLength(String key) throws IOException {
        return getFileStatus(key).size;
    }

    @Override
    public boolean upload(InputStream stream, String key, boolean overwrite) throws IOException {
        if (!overwrite && mockFileMap.containsKey(key)) {
            return false;
        }
        byte[] data = IOUtils.readFullyToByteArray(new DataInputStream(stream));
        MockFile mockFile = new MockFile(key, System.currentTimeMillis(), data);
        mockFileMap.put(key, mockFile);
        return true;
    }

    @Override
    public InputStream fetch(String key, long offset, int size) throws IOException {
        MockFile file = mockFileMap.get(key);
        if (file == null) {
            throw new IOException("key not found: " + key);
        }
        return file.toInputStream(offset, size);
    }

    @Override
    public MyFileInfo listOneStatus(String keyPrefix) throws IOException {
        List<MyFileInfo> fileInfos = listNStatus(keyPrefix, 1);
        if (fileInfos.isEmpty()) {
            return null;
        }
        return fileInfos.get(0);
    }

    @Override
    public List<MyFileInfo> listNStatus(String keyPrefix, int n) throws IOException {
        return mockFileMap.entrySet()
                .stream()
                .filter(entry -> entry.getKey().startsWith(keyPrefix))
                .sorted(Map.Entry.comparingByKey())
                .limit(n)
                .map(entry -> entry.getValue().toMyFileInfo())
                .collect(Collectors.toList());
    }

    @Override
    public List<MyFileInfo> listStatus(String key, boolean useDirectory) throws IOException {
        ArrayList<String> commonPrefixes = new ArrayList<>();
        ArrayList<MyFileInfo> fileInfos = new ArrayList<>();
        for (Map.Entry<String, MockFile> entry : mockFileMap.entrySet()) {
            String entryKey = entry.getKey();
            if (entryKey.startsWith(key)) {
                if (entryKey.equals(key)) {
                    if (useDirectory) {
                        commonPrefixes.add(key);
                    } else {
                        fileInfos.add(entry.getValue().toMyFileInfo());
                    }
                } else {
                    int slashIndex = entryKey.indexOf('/', key.length());
                    if (slashIndex == -1) {
                        fileInfos.add(entry.getValue().toMyFileInfo());
                    } else {
                        String prefix = entryKey.substring(0, slashIndex + 1);
                        if (!commonPrefixes.contains(prefix)) {
                            commonPrefixes.add(prefix);
                        }
                    }
                }
            }
        }
        if (useDirectory) {
            for (String prefix : commonPrefixes) {
                fileInfos.add(new MyFileInfo(prefix, 0, 0));
            }
        }
        fileInfos.removeIf(fileInfo -> fileInfo.key.equals(key));
        return fileInfos;
    }

    @Override
    public RemoteIterator<MyFileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException {
        return RemoteIterators.remoteIteratorFromIterable(listStatus(prefixKey, useDirectory));
    }

    @Override
    public boolean copyKey(String oldKey, String newKey) throws IOException {
        MockFile oldFile = mockFileMap.get(oldKey);
        if (oldFile == null) {
            return false;
        }
        MockFile newFile = new MockFile(newKey, System.currentTimeMillis(), oldFile.data.clone());
        mockFileMap.put(newKey, newFile);
        return true;
    }

    @Override
    public boolean copyKeys(String oldPrefix, String newPrefix) throws IOException {
        for (String key : mockFileMap.keySet()) {
            if (key.startsWith(oldPrefix)) {
                String newKey = newPrefix + key.substring(oldPrefix.length());
                if (!copyKey(key, newKey)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean renameKey(String oldKey, String newKey) throws IOException {
        MockFile oldFile = mockFileMap.remove(oldKey);
        if (oldFile == null) {
            return false;
        }
        MockFile newFile = new MockFile(newKey, System.currentTimeMillis(), oldFile.data.clone());
        mockFileMap.put(newKey, newFile);
        return true;
    }

    @Override
    public boolean renameKeys(String oldPrefix, String newPrefix) throws IOException {
        for (String key : mockFileMap.keySet()) {
            if (key.startsWith(oldPrefix)) {
                String newKey = newPrefix + key.substring(oldPrefix.length());
                if (!renameKey(key, newKey)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean deleteKey(String key) throws IOException {
        return mockFileMap.remove(key) != null;
    }

    @Override
    public boolean deleteKeys(String prefix) throws IOException {
        for (String key : mockFileMap.keySet()) {
            if (key.startsWith(prefix)) {
                if (!deleteKey(key)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean makeEmptyObject(String key) throws IOException {
        if (mockFileMap.containsKey(key)) {
            return false;
        }
        MockFile file = new MockFile(key, System.currentTimeMillis(), new byte[0]);
        mockFileMap.put(key, file);
        return true;
    }

    @Override
    public MyFileInfo getFileStatus(String key) throws IOException {
        if (mockFileMap.containsKey(key)) {
            return mockFileMap.get(key).toMyFileInfo();
        }
        return null;
    }
}
