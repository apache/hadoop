package org.apache.hadoop.fs.qiniu.kodo;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.qiniu.kodo.client.IQiniuKodoClient;
import org.apache.hadoop.fs.qiniu.kodo.client.QiniuKodoFileInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
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

        public QiniuKodoFileInfo toMyFileInfo() {
            return new QiniuKodoFileInfo(key, data.length, putTime);
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
    public void upload(InputStream stream, String key, boolean overwrite) throws IOException {
        if (!overwrite && mockFileMap.containsKey(key)) {
            throw new IOException("key already exists: " + key);
        }
        byte[] data = IOUtils.readFullyToByteArray(new DataInputStream(stream));
        MockFile mockFile = new MockFile(key, System.currentTimeMillis(), data);
        mockFileMap.put(key, mockFile);
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
    public QiniuKodoFileInfo listOneStatus(String keyPrefix) throws IOException {
        List<QiniuKodoFileInfo> fileInfos = listNStatus(keyPrefix, 1);
        if (fileInfos.isEmpty()) {
            return null;
        }
        return fileInfos.get(0);
    }

    @Override
    public List<QiniuKodoFileInfo> listNStatus(String keyPrefix, int n) throws IOException {
        return mockFileMap.entrySet()
                .stream()
                .filter(entry -> entry.getKey().startsWith(keyPrefix))
                .sorted(Map.Entry.comparingByKey())
                .limit(n)
                .map(entry -> entry.getValue().toMyFileInfo())
                .collect(Collectors.toList());
    }


    private static String getCommonPrefixByTrieNode(TrieTree.TrieNode<?> node, String delimiter) {
        TrieTree.TrieNode<?> n = node;
        List<String> prefixParts = new ArrayList<>();
        while (n != null) {
            prefixParts.add(n.name);
            n = n.parent;
        }
        Collections.reverse(prefixParts);
        return String.join(delimiter, prefixParts);
    }

    @Override
    public List<QiniuKodoFileInfo> listStatus(String key, boolean useDirectory) throws IOException {
        List<QiniuKodoFileInfo> allPrefixFiles = listNStatus(key, Integer.MAX_VALUE);
        if (!useDirectory) {
            return allPrefixFiles;
        }
        String delimiter = "/";
        TrieTree<QiniuKodoFileInfo> trie = new TrieTree<>();
        for (QiniuKodoFileInfo fileInfo : allPrefixFiles) {
            String[] keyParts = fileInfo.key.split(delimiter);
            trie.insert(Arrays.asList(keyParts), fileInfo);
        }

        TrieTree.TrieNode<QiniuKodoFileInfo> result = trie.search(Arrays.asList(key.isEmpty() ? new String[0] : key.split(delimiter)));
        if (result == null) {
            return Collections.emptyList();
        }

        List<String> commonPrefixes = new ArrayList<>();
        List<QiniuKodoFileInfo> files = new ArrayList<>();
        for (TrieTree.TrieNode<QiniuKodoFileInfo> node : result.children) {
            if (node.value != null) {
                files.add(node.value);
            } else {
                commonPrefixes.add(getCommonPrefixByTrieNode(node, delimiter));
            }
        }
        for (String prefix : commonPrefixes) {
            files.add(new QiniuKodoFileInfo(prefix, 0, 0));
        }
        return files;
    }

    @Override
    public RemoteIterator<QiniuKodoFileInfo> listStatusIterator(String prefixKey, boolean useDirectory) throws IOException {
        return RemoteIterators.remoteIteratorFromIterable(listStatus(prefixKey, useDirectory));
    }

    @Override
    public void copyKey(String oldKey, String newKey) throws IOException {
        MockFile oldFile = mockFileMap.get(oldKey);
        if (oldFile == null) {
            throw new IOException("key not found: " + oldKey);
        }
        MockFile newFile = new MockFile(newKey, System.currentTimeMillis(), oldFile.data.clone());
        mockFileMap.put(newKey, newFile);
    }

    @Override
    public void copyKeys(String oldPrefix, String newPrefix) throws IOException {
        batchAction(oldPrefix, key -> {
            try {
                copyKey(key, key.replaceFirst(oldPrefix, newPrefix));
            } catch (IOException e) {
                return e;
            }
            return null;
        });
    }

    @Override
    public void renameKey(String oldKey, String newKey) throws IOException {
        MockFile oldFile = mockFileMap.remove(oldKey);
        if (oldFile == null) {
            throw new IOException("key not found: " + oldKey);
        }
        MockFile newFile = new MockFile(newKey, System.currentTimeMillis(), oldFile.data.clone());
        mockFileMap.put(newKey, newFile);
    }

    @Override
    public void renameKeys(String oldPrefix, String newPrefix) throws IOException {
        batchAction(oldPrefix, key -> {
            try {
                renameKey(key, key.replaceFirst(oldPrefix, newPrefix));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    @Override
    public void deleteKey(String key) throws IOException {
        if (mockFileMap.remove(key) == null) {
            throw new IOException("key not found: " + key);
        }
    }

    @Override
    public void deleteKeys(String prefix) throws IOException {
        batchAction(prefix, key -> {
            try {
                deleteKey(key);
            } catch (IOException e) {
                return e;
            }
            return null;
        });
    }

    private void batchAction(String prefix, Function<String, IOException> action) throws IOException {
        for (String key : mockFileMap.keySet()) {
            if (key.startsWith(prefix)) {
                IOException e = action.apply(key);
                if (e != null) {
                    throw e;
                }
            }
        }
    }

    @Override
    public void makeEmptyObject(String key) throws IOException {
        if (mockFileMap.containsKey(key)) {
            throw new IOException("key already exists: " + key);
        }
        MockFile file = new MockFile(key, System.currentTimeMillis(), new byte[0]);
        mockFileMap.put(key, file);
    }

    @Override
    public QiniuKodoFileInfo getFileStatus(String key) throws IOException {
        if (mockFileMap.containsKey(key)) {
            return mockFileMap.get(key).toMyFileInfo();
        }
        return null;
    }
}
