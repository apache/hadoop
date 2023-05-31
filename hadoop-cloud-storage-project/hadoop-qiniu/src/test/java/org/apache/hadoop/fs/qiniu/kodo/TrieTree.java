package org.apache.hadoop.fs.qiniu.kodo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TrieTree<T> {
    public static class TrieNode<T> {
        public String name;
        public T value;
        public Set<TrieNode<T>> children;
        public TrieNode<T> parent;
    }

    private final TrieNode<T> root;

    public TrieTree() {
        this.root = new TrieNode<T>();
        this.root.children = new HashSet<>();
    }

    // 插入到前缀树中
    public void insert(List<String> path, T value) {
        TrieNode<T> node = root;
        for (String name : path) {
            TrieNode<T> child = null;
            // 查找是否已经存在该节点
            for (TrieNode<T> n : node.children) {
                // 如果已经存在，则直接使用该节点
                if (n.name.equals(name)) {
                    child = n;
                    break;
                }
            }
            // 如果没有找到，则创建一个新的节点
            if (child == null) {
                child = new TrieNode<T>();
                child.name = name;
                child.children = new HashSet<>();
                child.parent = node;
                node.children.add(child);
            }
            // 继续向下查找
            node = child;
        }
        // 设置节点的值
        node.value = value;
    }

    // 在前缀树中查找该前缀数组
    public TrieNode<T> search(List<String> path) {
        TrieNode<T> node = root;
        for (String name : path) {
            TrieNode<T> child = null;
            for (TrieNode<T> n : node.children) {
                if (n.name.equals(name)) {
                    child = n;
                    break;
                }
            }
            // 如果没有找到，则直接返回null
            if (child == null) {
                return null;
            }
            node = child;
        }
        return node;
    }
}
