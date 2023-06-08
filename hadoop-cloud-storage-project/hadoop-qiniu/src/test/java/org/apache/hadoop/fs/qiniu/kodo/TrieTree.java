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

    public void insert(List<String> path, T value) {
        TrieNode<T> node = root;
        for (String name : path) {
            TrieNode<T> child = null;
            for (TrieNode<T> n : node.children) {
                if (n.name.equals(name)) {
                    child = n;
                    break;
                }
            }
            if (child == null) {
                child = new TrieNode<T>();
                child.name = name;
                child.children = new HashSet<>();
                child.parent = node;
                node.children.add(child);
            }
            node = child;
        }
        node.value = value;
    }

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
            if (child == null) {
                return null;
            }
            node = child;
        }
        return node;
    }
}
