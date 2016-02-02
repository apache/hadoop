/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.util;

import org.apache.hadoop.util.Time;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.SortedSet;

/**
 * A memory efficient implementation of RBTree. Instead of having a Node for
 * each entry each node contains an array holding 64 entries.
 *
 * Based on the Apache Harmony folded TreeMap.
 *
 * @param <E> Entry type
 */
public class FoldedTreeSet<E> implements SortedSet<E> {

  private static final boolean RED = true;
  private static final boolean BLACK = false;

  private final Comparator<E> comparator;
  private Node<E> root;
  private int size;
  private int nodeCount;
  private int modCount;
  private Node<E> cachedNode;

  /**
   * Internal tree node that holds a sorted array of entries.
   *
   * @param <E> type of the elements
   */
  private static class Node<E> {

    private static final int NODE_SIZE = 64;

    // Tree structure
    private Node<E> parent, left, right;
    private boolean color;
    private final E[] entries;
    private int leftIndex = 0, rightIndex = -1;
    private int size = 0;
    // List for fast ordered iteration
    private Node<E> prev, next;

    @SuppressWarnings("unchecked")
    public Node() {
      entries = (E[]) new Object[NODE_SIZE];
    }

    public boolean isRed() {
      return color == RED;
    }

    public boolean isBlack() {
      return color == BLACK;
    }

    public Node<E> getLeftMostNode() {
      Node<E> node = this;
      while (node.left != null) {
        node = node.left;
      }
      return node;
    }

    public Node<E> getRightMostNode() {
      Node<E> node = this;
      while (node.right != null) {
        node = node.right;
      }
      return node;
    }

    public void addEntryLeft(E entry) {
      assert rightIndex < entries.length;
      assert !isFull();

      if (leftIndex == 0) {
        rightIndex++;
        // Shift entries right/up
        System.arraycopy(entries, 0, entries, 1, size);
      } else {
        leftIndex--;
      }
      size++;
      entries[leftIndex] = entry;
    }

    public void addEntryRight(E entry) {
      assert !isFull();

      if (rightIndex == NODE_SIZE - 1) {
        assert leftIndex > 0;
        // Shift entries left/down
        System.arraycopy(entries, leftIndex, entries, --leftIndex, size);
      } else {
        rightIndex++;
      }
      size++;
      entries[rightIndex] = entry;
    }

    public void addEntryAt(E entry, int index) {
      assert !isFull();

      if (leftIndex == 0 || ((rightIndex != Node.NODE_SIZE - 1)
                             && (rightIndex - index <= index - leftIndex))) {
        rightIndex++;
        System.arraycopy(entries, index,
                         entries, index + 1, rightIndex - index);
        entries[index] = entry;
      } else {
        int newLeftIndex = leftIndex - 1;
        System.arraycopy(entries, leftIndex,
                         entries, newLeftIndex, index - leftIndex);
        leftIndex = newLeftIndex;
        entries[index - 1] = entry;
      }
      size++;
    }

    public void addEntriesLeft(Node<E> from) {
      leftIndex -= from.size;
      size += from.size;
      System.arraycopy(from.entries, from.leftIndex,
                       entries, leftIndex, from.size);
    }

    public void addEntriesRight(Node<E> from) {
      System.arraycopy(from.entries, from.leftIndex,
                       entries, rightIndex + 1, from.size);
      size += from.size;
      rightIndex += from.size;
    }

    public E insertEntrySlideLeft(E entry, int index) {
      E pushedEntry = entries[0];
      System.arraycopy(entries, 1, entries, 0, index - 1);
      entries[index - 1] = entry;
      return pushedEntry;
    }

    public E insertEntrySlideRight(E entry, int index) {
      E movedEntry = entries[rightIndex];
      System.arraycopy(entries, index, entries, index + 1, rightIndex - index);
      entries[index] = entry;
      return movedEntry;
    }

    public E removeEntryLeft() {
      assert !isEmpty();
      E entry = entries[leftIndex];
      entries[leftIndex] = null;
      leftIndex++;
      size--;
      return entry;
    }

    public E removeEntryRight() {
      assert !isEmpty();
      E entry = entries[rightIndex];
      entries[rightIndex] = null;
      rightIndex--;
      size--;
      return entry;
    }

    public E removeEntryAt(int index) {
      assert !isEmpty();

      E entry = entries[index];
      int rightSize = rightIndex - index;
      int leftSize = index - leftIndex;
      if (rightSize <= leftSize) {
        System.arraycopy(entries, index + 1, entries, index, rightSize);
        entries[rightIndex] = null;
        rightIndex--;
      } else {
        System.arraycopy(entries, leftIndex, entries, leftIndex + 1, leftSize);
        entries[leftIndex] = null;
        leftIndex++;
      }
      size--;
      return entry;
    }

    public boolean isFull() {
      return size == NODE_SIZE;
    }

    public boolean isEmpty() {
      return size == 0;
    }

    public void clear() {
      if (leftIndex < rightIndex) {
        Arrays.fill(entries, leftIndex, rightIndex + 1, null);
      }
      size = 0;
      leftIndex = 0;
      rightIndex = -1;
      prev = null;
      next = null;
      parent = null;
      left = null;
      right = null;
      color = BLACK;
    }
  }

  private static final class TreeSetIterator<E> implements Iterator<E> {

    private final FoldedTreeSet<E> tree;
    private int iteratorModCount;
    private Node<E> node;
    private int index;
    private E lastEntry;
    private int lastIndex;
    private Node<E> lastNode;

    private TreeSetIterator(FoldedTreeSet<E> tree) {
      this.tree = tree;
      this.iteratorModCount = tree.modCount;
      if (!tree.isEmpty()) {
        this.node = tree.root.getLeftMostNode();
        this.index = this.node.leftIndex;
      }
    }

    @Override
    public boolean hasNext() {
      checkForModification();
      return node != null;
    }

    @Override
    public E next() {
      if (hasNext()) {
        lastEntry = node.entries[index];
        lastIndex = index;
        lastNode = node;
        if (++index > node.rightIndex) {
          node = node.next;
          if (node != null) {
            index = node.leftIndex;
          }
        }
        return lastEntry;
      } else {
        throw new NoSuchElementException("Iterator exhausted");
      }
    }

    @Override
    public void remove() {
      if (lastEntry == null) {
        throw new IllegalStateException("No current element");
      }
      checkForModification();
      if (lastNode.size == 1) {
        // Safe to remove lastNode, the iterator is on the next node
        tree.deleteNode(lastNode);
      } else if (lastNode.leftIndex == lastIndex) {
        // Safe to remove leftmost entry, the iterator is on the next index
        lastNode.removeEntryLeft();
      } else if (lastNode.rightIndex == lastIndex) {
        // Safe to remove the rightmost entry, the iterator is on the next node
        lastNode.removeEntryRight();
      } else {
        // Remove entry in the middle of the array
        assert node == lastNode;
        int oldRIndex = lastNode.rightIndex;
        lastNode.removeEntryAt(lastIndex);
        if (oldRIndex > lastNode.rightIndex) {
          // Entries moved to the left in the array so index must be reset
          index = lastIndex;
        }
      }
      lastEntry = null;
      iteratorModCount++;
      tree.modCount++;
      tree.size--;
    }

    private void checkForModification() {
      if (iteratorModCount != tree.modCount) {
        throw new ConcurrentModificationException("Tree has been modified "
                                                  + "outside of iterator");
      }
    }
  }

  /**
   * Create a new TreeSet that uses the natural ordering of objects. The element
   * type must implement Comparable.
   */
  public FoldedTreeSet() {
    this(null);
  }

  /**
   * Create a new TreeSet that orders the elements using the supplied
   * Comparator.
   *
   * @param comparator Comparator able to compare elements of type E
   */
  public FoldedTreeSet(Comparator<E> comparator) {
    this.comparator = comparator;
  }

  private Node<E> cachedOrNewNode(E entry) {
    Node<E> node = (cachedNode != null) ? cachedNode : new Node<E>();
    cachedNode = null;
    nodeCount++;
    // Since BlockIDs are always increasing for new blocks it is best to
    // add values on the left side to enable quicker inserts on the right
    node.addEntryLeft(entry);
    return node;
  }

  private void cacheAndClear(Node<E> node) {
    if (cachedNode == null) {
      node.clear();
      cachedNode = node;
    }
  }

  @Override
  public Comparator<? super E> comparator() {
    return comparator;
  }

  @Override
  public SortedSet<E> subSet(E fromElement, E toElement) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SortedSet<E> headSet(E toElement) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SortedSet<E> tailSet(E fromElement) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public E first() {
    if (!isEmpty()) {
      Node<E> node = root.getLeftMostNode();
      return node.entries[node.leftIndex];
    }
    return null;
  }

  @Override
  public E last() {
    if (!isEmpty()) {
      Node<E> node = root.getRightMostNode();
      return node.entries[node.rightIndex];
    }
    return null;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return root == null;
  }

  /**
   * Lookup and return a stored object using a user provided comparator.
   *
   * @param obj Lookup key
   * @param cmp User provided Comparator. The comparator should expect that the
   *            proved obj will always be the first method parameter and any
   *            stored object will be the second parameter.
   *
   * @return A matching stored object or null if non is found
   */
  public E get(Object obj, Comparator<?> cmp) {
    Objects.requireNonNull(obj);

    Node<E> node = root;
    while (node != null) {
      E[] entries = node.entries;

      int leftIndex = node.leftIndex;
      int result = compare(obj, entries[leftIndex], cmp);
      if (result < 0) {
        node = node.left;
      } else if (result == 0) {
        return entries[leftIndex];
      } else {
        int rightIndex = node.rightIndex;
        if (leftIndex != rightIndex) {
          result = compare(obj, entries[rightIndex], cmp);
        }
        if (result == 0) {
          return entries[rightIndex];
        } else if (result > 0) {
          node = node.right;
        } else {
          int low = leftIndex + 1;
          int high = rightIndex - 1;
          while (low <= high) {
            int mid = (low + high) >>> 1;
            result = compare(obj, entries[mid], cmp);
            if (result > 0) {
              low = mid + 1;
            } else if (result < 0) {
              high = mid - 1;
            } else {
              return entries[mid];
            }
          }
          return null;
        }
      }
    }
    return null;
  }

  /**
   * Lookup and return a stored object.
   *
   * @param entry Lookup entry
   *
   * @return A matching stored object or null if non is found
   */
  public E get(E entry) {
    return get(entry, comparator);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean contains(Object obj) {
    return get((E) obj) != null;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int compare(Object lookup, Object stored, Comparator cmp) {
    return cmp != null
           ? cmp.compare(lookup, stored)
           : ((Comparable<Object>) lookup).compareTo(stored);
  }

  @Override
  public Iterator<E> iterator() {
    return new TreeSetIterator<>(this);
  }

  @Override
  public Object[] toArray() {
    Object[] objects = new Object[size];
    if (!isEmpty()) {
      int pos = 0;
      for (Node<E> node = root.getLeftMostNode(); node != null;
          pos += node.size, node = node.next) {
        System.arraycopy(node.entries, node.leftIndex, objects, pos, node.size);
      }
    }
    return objects;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(T[] a) {
    T[] r = a.length >= size ? a
            : (T[]) java.lang.reflect.Array
        .newInstance(a.getClass().getComponentType(), size);
    if (!isEmpty()) {
      Node<E> node = root.getLeftMostNode();
      int pos = 0;
      while (node != null) {
        System.arraycopy(node.entries, node.leftIndex, r, pos, node.size);
        pos += node.size;
        node = node.next;
      }
      if (r.length > pos) {
        r[pos] = null;
      }
    } else if (a.length > 0) {
      a[0] = null;
    }
    return r;
  }

  /**
   * Add or replace an entry in the TreeSet.
   *
   * @param entry Entry to add or replace/update.
   *
   * @return the previous entry, or null if this set did not already contain the
   *         specified entry
   */
  public E addOrReplace(E entry) {
    return add(entry, true);
  }

  @Override
  public boolean add(E entry) {
    return add(entry, false) == null;
  }

  /**
   * Internal add method to add a entry to the set.
   *
   * @param entry   Entry to add
   * @param replace Should the entry replace an old entry which is equal to the
   *                new entry
   *
   * @return null if entry added and didn't exist or the previous value (which
   *         might not have been overwritten depending on the replace parameter)
   */
  private E add(E entry, boolean replace) {
    Objects.requireNonNull(entry);

    // Empty tree
    if (isEmpty()) {
      root = cachedOrNewNode(entry);
      size = 1;
      modCount++;
      return null;
    }

    // Compare right entry first since inserts of comperatively larger entries
    // is more likely to be inserted. BlockID is always increasing in HDFS.
    Node<E> node = root;
    Node<E> prevNode = null;
    int result = 0;
    while (node != null) {
      prevNode = node;
      E[] entries = node.entries;
      int rightIndex = node.rightIndex;
      result = compare(entry, entries[rightIndex], comparator);
      if (result > 0) {
        node = node.right;
      } else if (result == 0) {
        E prevEntry = entries[rightIndex];
        if (replace) {
          entries[rightIndex] = entry;
        }
        return prevEntry;
      } else {
        int leftIndex = node.leftIndex;
        if (leftIndex != rightIndex) {
          result = compare(entry, entries[leftIndex], comparator);
        }
        if (result < 0) {
          node = node.left;
        } else if (result == 0) {
          E prevEntry = entries[leftIndex];
          if (replace) {
            entries[leftIndex] = entry;
          }
          return prevEntry;
        } else {
          // Insert in this node
          int low = leftIndex + 1, high = rightIndex - 1;
          while (low <= high) {
            int mid = (low + high) >>> 1;
            result = compare(entry, entries[mid], comparator);
            if (result > 0) {
              low = mid + 1;
            } else if (result == 0) {
              E prevEntry = entries[mid];
              if (replace) {
                entries[mid] = entry;
              }
              return prevEntry;
            } else {
              high = mid - 1;
            }
          }
          addElementInNode(node, entry, low);
          return null;
        }
      }
    }

    assert prevNode != null;
    size++;
    modCount++;
    if (!prevNode.isFull()) {
      // The previous node still has space
      if (result < 0) {
        prevNode.addEntryLeft(entry);
      } else {
        prevNode.addEntryRight(entry);
      }
    } else if (result < 0) {
      // The previous node is full, add to adjencent node or a new node
      if (prevNode.prev != null && !prevNode.prev.isFull()) {
        prevNode.prev.addEntryRight(entry);
      } else {
        attachNodeLeft(prevNode, cachedOrNewNode(entry));
      }
    } else if (prevNode.next != null && !prevNode.next.isFull()) {
      prevNode.next.addEntryLeft(entry);
    } else {
      attachNodeRight(prevNode, cachedOrNewNode(entry));
    }
    return null;
  }

  /**
   * Insert an entry last in the sorted tree. The entry must be the considered
   * larger than the currently largest entry in the set when doing
   * current.compareTo(entry), if entry is not the largest entry the method will
   * fall back on the regular add method.
   *
   * @param entry entry to add
   *
   * @return True if added, false if already existed in the set
   */
  public boolean addSortedLast(E entry) {

    if (isEmpty()) {
      root = cachedOrNewNode(entry);
      size = 1;
      modCount++;
      return true;
    } else {
      Node<E> node = root.getRightMostNode();
      if (compare(node.entries[node.rightIndex], entry, comparator) < 0) {
        size++;
        modCount++;
        if (!node.isFull()) {
          node.addEntryRight(entry);
        } else {
          attachNodeRight(node, cachedOrNewNode(entry));
        }
        return true;
      }
    }

    // Fallback on normal add if entry is unsorted
    return add(entry);
  }

  private void addElementInNode(Node<E> node, E entry, int index) {
    size++;
    modCount++;

    if (!node.isFull()) {
      node.addEntryAt(entry, index);
    } else {
      // Node is full, insert and push old entry
      Node<E> prev = node.prev;
      Node<E> next = node.next;
      if (prev == null) {
        // First check if we have space in the the next node
        if (next != null && !next.isFull()) {
          E movedEntry = node.insertEntrySlideRight(entry, index);
          next.addEntryLeft(movedEntry);
        } else {
          // Since prev is null the left child must be null
          assert node.left == null;
          E movedEntry = node.insertEntrySlideLeft(entry, index);
          Node<E> newNode = cachedOrNewNode(movedEntry);
          attachNodeLeft(node, newNode);
        }
      } else if (!prev.isFull()) {
        // Prev has space
        E movedEntry = node.insertEntrySlideLeft(entry, index);
        prev.addEntryRight(movedEntry);
      } else if (next == null) {
        // Since next is null the right child must be null
        assert node.right == null;
        E movedEntry = node.insertEntrySlideRight(entry, index);
        Node<E> newNode = cachedOrNewNode(movedEntry);
        attachNodeRight(node, newNode);
      } else if (!next.isFull()) {
        // Next has space
        E movedEntry = node.insertEntrySlideRight(entry, index);
        next.addEntryLeft(movedEntry);
      } else {
        // Both prev and next nodes exist and are full
        E movedEntry = node.insertEntrySlideRight(entry, index);
        Node<E> newNode = cachedOrNewNode(movedEntry);
        if (node.right == null) {
          attachNodeRight(node, newNode);
        } else {
          // Since our right node exist,
          // the left node of our next node must be empty
          assert next.left == null;
          attachNodeLeft(next, newNode);
        }
      }
    }
  }

  private void attachNodeLeft(Node<E> node, Node<E> newNode) {
    newNode.parent = node;
    node.left = newNode;

    newNode.next = node;
    newNode.prev = node.prev;
    if (newNode.prev != null) {
      newNode.prev.next = newNode;
    }
    node.prev = newNode;
    balanceInsert(newNode);
  }

  private void attachNodeRight(Node<E> node, Node<E> newNode) {
    newNode.parent = node;
    node.right = newNode;

    newNode.prev = node;
    newNode.next = node.next;
    if (newNode.next != null) {
      newNode.next.prev = newNode;
    }
    node.next = newNode;
    balanceInsert(newNode);
  }

  /**
   * Balance the RB Tree after insert.
   *
   * @param node Added node
   */
  private void balanceInsert(Node<E> node) {
    node.color = RED;

    while (node != root && node.parent.isRed()) {
      if (node.parent == node.parent.parent.left) {
        Node<E> uncle = node.parent.parent.right;
        if (uncle != null && uncle.isRed()) {
          node.parent.color = BLACK;
          uncle.color = BLACK;
          node.parent.parent.color = RED;
          node = node.parent.parent;
        } else {
          if (node == node.parent.right) {
            node = node.parent;
            rotateLeft(node);
          }
          node.parent.color = BLACK;
          node.parent.parent.color = RED;
          rotateRight(node.parent.parent);
        }
      } else {
        Node<E> uncle = node.parent.parent.left;
        if (uncle != null && uncle.isRed()) {
          node.parent.color = BLACK;
          uncle.color = BLACK;
          node.parent.parent.color = RED;
          node = node.parent.parent;
        } else {
          if (node == node.parent.left) {
            node = node.parent;
            rotateRight(node);
          }
          node.parent.color = BLACK;
          node.parent.parent.color = RED;
          rotateLeft(node.parent.parent);
        }
      }
    }
    root.color = BLACK;
  }

  private void rotateRight(Node<E> node) {
    Node<E> pivot = node.left;
    node.left = pivot.right;
    if (pivot.right != null) {
      pivot.right.parent = node;
    }
    pivot.parent = node.parent;
    if (node.parent == null) {
      root = pivot;
    } else if (node == node.parent.right) {
      node.parent.right = pivot;
    } else {
      node.parent.left = pivot;
    }
    pivot.right = node;
    node.parent = pivot;
  }

  private void rotateLeft(Node<E> node) {
    Node<E> pivot = node.right;
    node.right = pivot.left;
    if (pivot.left != null) {
      pivot.left.parent = node;
    }
    pivot.parent = node.parent;
    if (node.parent == null) {
      root = pivot;
    } else if (node == node.parent.left) {
      node.parent.left = pivot;
    } else {
      node.parent.right = pivot;
    }
    pivot.left = node;
    node.parent = pivot;
  }

  /**
   * Remove object using a provided comparator, and return the removed entry.
   *
   * @param obj Lookup entry
   * @param cmp User provided Comparator. The comparator should expect that the
   *            proved obj will always be the first method parameter and any
   *            stored object will be the second parameter.
   *
   * @return The removed entry or null if not found
   */
  public E removeAndGet(Object obj, Comparator<?> cmp) {
    Objects.requireNonNull(obj);

    if (!isEmpty()) {
      Node<E> node = root;
      while (node != null) {
        E[] entries = node.entries;
        int leftIndex = node.leftIndex;
        int result = compare(obj, entries[leftIndex], cmp);
        if (result < 0) {
          node = node.left;
        } else if (result == 0) {
          return removeElementLeft(node);
        } else {
          int rightIndex = node.rightIndex;
          if (leftIndex != rightIndex) {
            result = compare(obj, entries[rightIndex], cmp);
          }
          if (result == 0) {
            return removeElementRight(node);
          } else if (result > 0) {
            node = node.right;
          } else {
            int low = leftIndex + 1, high = rightIndex - 1;
            while (low <= high) {
              int mid = (low + high) >>> 1;
              result = compare(obj, entries[mid], cmp);
              if (result > 0) {
                low = mid + 1;
              } else if (result == 0) {
                return removeElementAt(node, mid);
              } else {
                high = mid - 1;
              }
            }
            return null;
          }
        }
      }
    }
    return null;
  }

  /**
   * Remove object and return the removed entry.
   *
   * @param obj Lookup entry
   *
   * @return The removed entry or null if not found
   */
  public E removeAndGet(Object obj) {
    return removeAndGet(obj, comparator);
  }

  /**
   * Remove object using a provided comparator.
   *
   * @param obj Lookup entry
   * @param cmp User provided Comparator. The comparator should expect that the
   *            proved obj will always be the first method parameter and any
   *            stored object will be the second parameter.
   *
   * @return True if found and removed, else false
   */
  public boolean remove(Object obj, Comparator<?> cmp) {
    return removeAndGet(obj, cmp) != null;
  }

  @Override
  public boolean remove(Object obj) {
    return removeAndGet(obj, comparator) != null;
  }

  private E removeElementLeft(Node<E> node) {
    modCount++;
    size--;
    E entry = node.removeEntryLeft();

    if (node.isEmpty()) {
      deleteNode(node);
    } else if (node.prev != null
               && (Node.NODE_SIZE - 1 - node.prev.rightIndex) >= node.size) {
      // Remaining entries fit in the prev node, move them and delete this node
      node.prev.addEntriesRight(node);
      deleteNode(node);
    } else if (node.next != null && node.next.leftIndex >= node.size) {
      // Remaining entries fit in the next node, move them and delete this node
      node.next.addEntriesLeft(node);
      deleteNode(node);
    } else if (node.prev != null && node.prev.size < node.leftIndex) {
      // Entries in prev node will fit in this node, move them and delete prev
      node.addEntriesLeft(node.prev);
      deleteNode(node.prev);
    }

    return entry;
  }

  private E removeElementRight(Node<E> node) {
    modCount++;
    size--;
    E entry = node.removeEntryRight();

    if (node.isEmpty()) {
      deleteNode(node);
    } else if (node.prev != null
               && (Node.NODE_SIZE - 1 - node.prev.rightIndex) >= node.size) {
      // Remaining entries fit in the prev node, move them and delete this node
      node.prev.addEntriesRight(node);
      deleteNode(node);
    } else if (node.next != null && node.next.leftIndex >= node.size) {
      // Remaining entries fit in the next node, move them and delete this node
      node.next.addEntriesLeft(node);
      deleteNode(node);
    } else if (node.next != null
               && node.next.size < (Node.NODE_SIZE - 1 - node.rightIndex)) {
      // Entries in next node will fit in this node, move them and delete next
      node.addEntriesRight(node.next);
      deleteNode(node.next);
    }

    return entry;
  }

  private E removeElementAt(Node<E> node, int index) {
    modCount++;
    size--;
    E entry = node.removeEntryAt(index);

    if (node.prev != null
        && (Node.NODE_SIZE - 1 - node.prev.rightIndex) >= node.size) {
      // Remaining entries fit in the prev node, move them and delete this node
      node.prev.addEntriesRight(node);
      deleteNode(node);
    } else if (node.next != null && (node.next.leftIndex) >= node.size) {
      // Remaining entries fit in the next node, move them and delete this node
      node.next.addEntriesLeft(node);
      deleteNode(node);
    } else if (node.prev != null && node.prev.size < node.leftIndex) {
      // Entries in prev node will fit in this node, move them and delete prev
      node.addEntriesLeft(node.prev);
      deleteNode(node.prev);
    } else if (node.next != null
               && node.next.size < (Node.NODE_SIZE - 1 - node.rightIndex)) {
      // Entries in next node will fit in this node, move them and delete next
      node.addEntriesRight(node.next);
      deleteNode(node.next);
    }

    return entry;
  }

  /**
   * Delete the node and ensure the tree is balanced.
   *
   * @param node node to delete
   */
  private void deleteNode(final Node<E> node) {
    if (node.right == null) {
      if (node.left != null) {
        attachToParent(node, node.left);
      } else {
        attachNullToParent(node);
      }
    } else if (node.left == null) {
      attachToParent(node, node.right);
    } else {
      // node.left != null && node.right != null
      // node.next should replace node in tree
      // node.next != null guaranteed since node.left != null
      // node.next.left == null since node.next.prev is node
      // node.next.right may be null or non-null
      Node<E> toMoveUp = node.next;
      if (toMoveUp.right == null) {
        attachNullToParent(toMoveUp);
      } else {
        attachToParent(toMoveUp, toMoveUp.right);
      }
      toMoveUp.left = node.left;
      if (toMoveUp.left != null) {
        toMoveUp.left.parent = toMoveUp;
      }
      toMoveUp.right = node.right;
      if (toMoveUp.right != null) {
        toMoveUp.right.parent = toMoveUp;
      }
      attachToParentNoBalance(node, toMoveUp);
      toMoveUp.color = node.color;
    }

    // Remove node from ordered list of nodes
    if (node.prev != null) {
      node.prev.next = node.next;
    }
    if (node.next != null) {
      node.next.prev = node.prev;
    }

    nodeCount--;
    cacheAndClear(node);
  }

  private void attachToParentNoBalance(Node<E> toDelete, Node<E> toConnect) {
    Node<E> parent = toDelete.parent;
    toConnect.parent = parent;
    if (parent == null) {
      root = toConnect;
    } else if (toDelete == parent.left) {
      parent.left = toConnect;
    } else {
      parent.right = toConnect;
    }
  }

  private void attachToParent(Node<E> toDelete, Node<E> toConnect) {
    attachToParentNoBalance(toDelete, toConnect);
    if (toDelete.isBlack()) {
      balanceDelete(toConnect);
    }
  }

  private void attachNullToParent(Node<E> toDelete) {
    Node<E> parent = toDelete.parent;
    if (parent == null) {
      root = null;
    } else {
      if (toDelete == parent.left) {
        parent.left = null;
      } else {
        parent.right = null;
      }
      if (toDelete.isBlack()) {
        balanceDelete(parent);
      }
    }
  }

  /**
   * Balance tree after removing a node.
   *
   * @param node Node to balance after deleting another node
   */
  private void balanceDelete(Node<E> node) {
    while (node != root && node.isBlack()) {
      if (node == node.parent.left) {
        Node<E> sibling = node.parent.right;
        if (sibling == null) {
          node = node.parent;
          continue;
        }
        if (sibling.isRed()) {
          sibling.color = BLACK;
          node.parent.color = RED;
          rotateLeft(node.parent);
          sibling = node.parent.right;
          if (sibling == null) {
            node = node.parent;
            continue;
          }
        }
        if ((sibling.left == null || !sibling.left.isRed())
            && (sibling.right == null || !sibling.right.isRed())) {
          sibling.color = RED;
          node = node.parent;
        } else {
          if (sibling.right == null || !sibling.right.isRed()) {
            sibling.left.color = BLACK;
            sibling.color = RED;
            rotateRight(sibling);
            sibling = node.parent.right;
          }
          sibling.color = node.parent.color;
          node.parent.color = BLACK;
          sibling.right.color = BLACK;
          rotateLeft(node.parent);
          node = root;
        }
      } else {
        Node<E> sibling = node.parent.left;
        if (sibling == null) {
          node = node.parent;
          continue;
        }
        if (sibling.isRed()) {
          sibling.color = BLACK;
          node.parent.color = RED;
          rotateRight(node.parent);
          sibling = node.parent.left;
          if (sibling == null) {
            node = node.parent;
            continue;
          }
        }
        if ((sibling.left == null || sibling.left.isBlack())
            && (sibling.right == null || sibling.right.isBlack())) {
          sibling.color = RED;
          node = node.parent;
        } else {
          if (sibling.left == null || sibling.left.isBlack()) {
            sibling.right.color = BLACK;
            sibling.color = RED;
            rotateLeft(sibling);
            sibling = node.parent.left;
          }
          sibling.color = node.parent.color;
          node.parent.color = BLACK;
          sibling.left.color = BLACK;
          rotateRight(node.parent);
          node = root;
        }
      }
    }
    node.color = BLACK;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object entry : c) {
      if (!contains(entry)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    boolean modified = false;
    for (E entry : c) {
      if (add(entry)) {
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean modified = false;
    Iterator<E> it = iterator();
    while (it.hasNext()) {
      if (!c.contains(it.next())) {
        it.remove();
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;
    for (Object entry : c) {
      if (remove(entry)) {
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public void clear() {
    modCount++;
    if (!isEmpty()) {
      size = 0;
      nodeCount = 0;
      cacheAndClear(root);
      root = null;
    }
  }

  /**
   * Returns the current size divided by the capacity of the tree. A value
   * between 0.0 and 1.0, where 1.0 means that every allocated node in the tree
   * is completely full.
   *
   * An empty set will return 1.0
   *
   * @return the fill ratio of the tree
   */
  public double fillRatio() {
    if (nodeCount > 1) {
      // Count the last node as completely full since it can't be compacted
      return (size + (Node.NODE_SIZE - root.getRightMostNode().size))
             / (double) (nodeCount * Node.NODE_SIZE);
    }
    return 1.0;
  }

  /**
   * Compact all the entries to use the fewest number of nodes in the tree.
   *
   * Having a compact tree minimize memory usage, but can cause inserts to get
   * slower due to new nodes needs to be allocated as there is no space in any
   * of the existing nodes anymore for entries added in the middle of the set.
   *
   * Useful to do to reduce memory consumption and if the tree is know to not
   * change after compaction or mainly added to at either extreme.
   *
   * @param timeout Maximum time to spend compacting the tree set in
   *                milliseconds.
   *
   * @return true if compaction completed, false if aborted
   */
  public boolean compact(long timeout) {

    if (!isEmpty()) {
      long start = Time.monotonicNow();
      Node<E> node = root.getLeftMostNode();
      while (node != null) {
        if (node.prev != null && !node.prev.isFull()) {
          Node<E> prev = node.prev;
          int count = Math.min(Node.NODE_SIZE - prev.size, node.size);
          System.arraycopy(node.entries, node.leftIndex,
                           prev.entries, prev.rightIndex + 1, count);
          node.leftIndex += count;
          node.size -= count;
          prev.rightIndex += count;
          prev.size += count;
        }
        if (node.isEmpty()) {
          Node<E> temp = node.next;
          deleteNode(node);
          node = temp;
          continue;
        } else if (!node.isFull()) {
          if (node.leftIndex != 0) {
            System.arraycopy(node.entries, node.leftIndex,
                             node.entries, 0, node.size);
            Arrays.fill(node.entries, node.size, node.rightIndex + 1, null);
            node.leftIndex = 0;
            node.rightIndex = node.size - 1;
          }
        }
        node = node.next;

        if (Time.monotonicNow() - start > timeout) {
          return false;
        }
      }
    }

    return true;
  }
}
