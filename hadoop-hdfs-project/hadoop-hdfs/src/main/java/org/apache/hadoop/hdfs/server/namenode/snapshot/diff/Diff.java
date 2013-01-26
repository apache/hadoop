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
package org.apache.hadoop.hdfs.server.namenode.snapshot.diff;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * The difference between the current state and a previous state of a list.
 * 
 * Given a previous state of a set and a sequence of create, delete and modify
 * operations such that the current state of the set can be obtained by applying
 * the operations on the previous state, the following algorithm construct the
 * difference between the current state and the previous state of the set.
 * 
 * <pre>
 * Two lists are maintained in the algorithm:
 * - c-list for newly created elements
 * - d-list for the deleted elements
 *
 * Denote the state of an element by the following
 *   (0, 0): neither in c-list nor d-list
 *   (c, 0): in c-list but not in d-list
 *   (0, d): in d-list but not in c-list
 *   (c, d): in both c-list and d-list
 *
 * For each case below, ( , ) at the end shows the result state of the element.
 *
 * Case 1. Suppose the element i is NOT in the previous state.           (0, 0)
 *   1.1. create i in current: add it to c-list                          (c, 0)
 *   1.1.1. create i in current and then create: impossible
 *   1.1.2. create i in current and then delete: remove it from c-list   (0, 0)
 *   1.1.3. create i in current and then modify: replace it in c-list    (c', 0)
 *
 *   1.2. delete i from current: impossible
 *
 *   1.3. modify i in current: impossible
 *
 * Case 2. Suppose the element i is ALREADY in the previous state.       (0, 0)
 *   2.1. create i in current: impossible
 *
 *   2.2. delete i from current: add it to d-list                        (0, d)
 *   2.2.1. delete i from current and then create: add it to c-list      (c, d)
 *   2.2.2. delete i from current and then delete: impossible
 *   2.2.2. delete i from current and then modify: impossible
 *
 *   2.3. modify i in current: put it in both c-list and d-list          (c, d)
 *   2.3.1. modify i in current and then create: impossible
 *   2.3.2. modify i in current and then delete: remove it from c-list   (0, d)
 *   2.3.3. modify i in current and then modify: replace it in c-list    (c', d)
 * </pre>
 *
 * @param <K> The key type.
 * @param <E> The element type, which must implement {@link Element} interface.
 */
public class Diff<K, E extends Diff.Element<K>> {
  /** An interface for the elements in a {@link Diff}. */
  public static interface Element<K> extends Comparable<K> {
    /** @return the key of this object. */
    public K getKey();
  }

  /** An interface for passing a method in order to process elements. */
  public static interface Processor<E> {
    /** Process the given element. */
    public void process(E element);
  }

  /** Containing exactly one element. */
  public static class Container<E> {
    private final E element;

    private Container(E element) {
      this.element = element;
    }

    /** @return the element. */
    public E getElement() {
      return element;
    }
  }
  
  /** 
   * Undo information for some operations such as {@link Diff#delete(E)}
   * and {@link Diff#modify(Element, Element)}.
   */
  public static class UndoInfo<E> {
    private final int createdInsertionPoint;
    private final E trashed;
    private final Integer deletedInsertionPoint;
    
    private UndoInfo(final int createdInsertionPoint, final E trashed,
        final Integer deletedInsertionPoint) {
      this.createdInsertionPoint = createdInsertionPoint;
      this.trashed = trashed;
      this.deletedInsertionPoint = deletedInsertionPoint;
    }
    
    public E getTrashedElement() {
      return trashed;
    }
  }

  private static final int DEFAULT_ARRAY_INITIAL_CAPACITY = 4;

  /**
   * Search the element from the list.
   * @return -1 if the list is null; otherwise, return the insertion point
   *    defined in {@link Collections#binarySearch(List, Object)}.
   *    Note that, when the list is null, -1 is the correct insertion point.
   */
  protected static <K, E extends Comparable<K>> int search(
      final List<E> elements, final K name) {
    return elements == null? -1: Collections.binarySearch(elements, name);
  }

  private static <E> void remove(final List<E> elements, final int i,
      final E expected) {
    final E removed = elements.remove(-i - 1);
    Preconditions.checkState(removed == expected,
        "removed != expected=%s, removed=%s.", expected, removed);
  }

  /** c-list: element(s) created in current. */
  private List<E> created;
  /** d-list: element(s) deleted from current. */
  private List<E> deleted;
  
  protected Diff() {}

  protected Diff(final List<E> created, final List<E> deleted) {
    this.created = created;
    this.deleted = deleted;
  }

  /** @return the created list, which is never null. */
  protected List<E> getCreatedList() {
    return created == null? Collections.<E>emptyList(): created;
  }

  /** @return the deleted list, which is never null. */
  protected List<E> getDeletedList() {
    return deleted == null? Collections.<E>emptyList(): deleted;
  }

  /**
   * @return null if the element is not found;
   *         otherwise, return the element in the c-list.
   */
  public E searchCreated(final K name) {
    final int c = search(created, name);
    return c < 0 ? null : created.get(c);
  }
  
  /**
   * @return null if the element is not found;
   *         otherwise, return the element in the d-list.
   */
  public E searchDeleted(final K name) {
    final int d = search(deleted, name);
    return d < 0 ? null : deleted.get(d);
  }
  
  /**
   * Insert the element to created.
   * @param i the insertion point defined
   *          in {@link Collections#binarySearch(List, Object)}
   */
  private void insertCreated(final E element, final int i) {
    if (i >= 0) {
      throw new AssertionError("Element already exists: element=" + element
          + ", created=" + created);
    }
    if (created == null) {
      created = new ArrayList<E>(DEFAULT_ARRAY_INITIAL_CAPACITY);
    }
    created.add(-i - 1, element);
  }

  /**
   * Insert the element to deleted.
   * @param i the insertion point defined
   *          in {@link Collections#binarySearch(List, Object)}
   */
  private void insertDeleted(final E element, final int i) {
    if (i >= 0) {
      throw new AssertionError("Element already exists: element=" + element
          + ", deleted=" + deleted);
    }
    if (deleted == null) {
      deleted = new ArrayList<E>(DEFAULT_ARRAY_INITIAL_CAPACITY);
    }
    deleted.add(-i - 1, element);
  }

  /**
   * Create an element in current state.
   * @return the c-list insertion point for undo.
   */
  public int create(final E element) {
    final int c = search(created, element.getKey());
    insertCreated(element, c);
    return c;
  }

  /**
   * Undo the previous {@link #create(E)} operation. Note that the behavior is
   * undefined if the previous operation is not {@link #create(E)}.
   */
  public void undoCreate(final E element, final int insertionPoint) {
    remove(created, insertionPoint, element);
  }

  /**
   * Delete an element from current state.
   * @return the undo information.
   */
  public UndoInfo<E> delete(final E element) {
    final int c = search(created, element.getKey());
    E previous = null;
    Integer d = null;
    if (c >= 0) {
      // remove a newly created element
      previous = created.remove(c);
    } else {
      // not in c-list, it must be in previous
      d = search(deleted, element.getKey());
      insertDeleted(element, d);
    }
    return new UndoInfo<E>(c, previous, d);
  }
  
  /**
   * Undo the previous {@link #delete(E)} operation. Note that the behavior is
   * undefined if the previous operation is not {@link #delete(E)}.
   */
  public void undoDelete(final E element, final UndoInfo<E> undoInfo) {
    final int c = undoInfo.createdInsertionPoint;
    if (c >= 0) {
      created.add(c, undoInfo.trashed);
    } else {
      remove(deleted, undoInfo.deletedInsertionPoint, element);
    }
  }

  /**
   * Modify an element in current state.
   * @return the undo information.
   */
  public UndoInfo<E> modify(final E oldElement, final E newElement) {
    Preconditions.checkArgument(oldElement != newElement,
        "They are the same object: oldElement == newElement = %s", newElement);
    Preconditions.checkArgument(oldElement.equals(newElement),
        "The names do not match: oldElement=%s, newElement=%s",
        oldElement, newElement);
    final int c = search(created, newElement.getKey());
    E previous = null;
    Integer d = null;
    if (c >= 0) {
      // Case 1.1.3 and 2.3.3: element is already in c-list,
      previous = created.set(c, newElement);
      
      //TODO: fix a bug that previous != oldElement.Set it to oldElement for now
      previous = oldElement;
    } else {
      d = search(deleted, oldElement.getKey());
      if (d < 0) {
        // Case 2.3: neither in c-list nor d-list
        insertCreated(newElement, c);
        insertDeleted(oldElement, d);
      }
    }
    return new UndoInfo<E>(c, previous, d);
  }

  /**
   * Undo the previous {@link #modify(E, E)} operation. Note that the behavior
   * is undefined if the previous operation is not {@link #modify(E, E)}.
   */
  public void undoModify(final E oldElement, final E newElement,
      final UndoInfo<E> undoInfo) {
    final int c = undoInfo.createdInsertionPoint;
    if (c >= 0) {
      created.set(c, undoInfo.trashed);
    } else {
      final int d = undoInfo.deletedInsertionPoint;
      if (d < 0) {
        remove(created, c, newElement);
        remove(deleted, d, oldElement);
      }
    }
  }

  /**
   * Find an element in the previous state.
   * 
   * @return null if the element cannot be determined in the previous state
   *         since no change is recorded and it should be determined in the
   *         current state; otherwise, return a {@link Container} containing the
   *         element in the previous state. Note that the element can possibly
   *         be null which means that the element is not found in the previous
   *         state.
   */
  public Container<E> accessPrevious(final K name) {
    return accessPrevious(name, created, deleted);
  }

  private static <K, E extends Diff.Element<K>> Container<E> accessPrevious(
      final K name, final List<E> clist, final List<E> dlist) {
    final int d = search(dlist, name);
    if (d >= 0) {
      // the element was in previous and was once deleted in current.
      return new Container<E>(dlist.get(d));
    } else {
      final int c = search(clist, name);
      // When c >= 0, the element in current is a newly created element.
      return c < 0? null: new Container<E>(null);
    }
  }

  /**
   * Find an element in the current state.
   * 
   * @return null if the element cannot be determined in the current state since
   *         no change is recorded and it should be determined in the previous
   *         state; otherwise, return a {@link Container} containing the element in
   *         the current state. Note that the element can possibly be null which
   *         means that the element is not found in the current state.
   */
  public Container<E> accessCurrent(K name) {
    return accessPrevious(name, deleted, created);
  }

  /**
   * Apply this diff to previous state in order to obtain current state.
   * @return the current state of the list.
   */
  public List<E> apply2Previous(final List<E> previous) {
    return apply2Previous(previous, getCreatedList(), getDeletedList());
  }

  private static <K, E extends Diff.Element<K>> List<E> apply2Previous(
      final List<E> previous, final List<E> clist, final List<E> dlist) {
    final List<E> current = new ArrayList<E>(previous);
    for(E d : dlist) {
      current.remove(d);
    }
    for(E c : clist) {
      final int i = search(current, c.getKey());
      current.add(-i - 1, c);
    }
    return current;
  }

  /**
   * Apply the reverse of this diff to current state in order
   * to obtain the previous state.
   * @return the previous state of the list.
   */
  public List<E> apply2Current(final List<E> current) {
    return apply2Previous(current, getDeletedList(), getCreatedList());
  }
  
  /**
   * Combine this diff with a posterior diff.  We have the following cases:
   * 
   * <pre>
   * 1. For (c, 0) in the posterior diff, check the element in this diff:
   * 1.1 (c', 0)  in this diff: impossible
   * 1.2 (0, d')  in this diff: put in c-list --> (c, d')
   * 1.3 (c', d') in this diff: impossible
   * 1.4 (0, 0)   in this diff: put in c-list --> (c, 0)
   * This is the same logic as {@link #create(E)}.
   * 
   * 2. For (0, d) in the posterior diff,
   * 2.1 (c', 0)  in this diff: remove from c-list --> (0, 0)
   * 2.2 (0, d')  in this diff: impossible
   * 2.3 (c', d') in this diff: remove from c-list --> (0, d')
   * 2.4 (0, 0)   in this diff: put in d-list --> (0, d)
   * This is the same logic as {@link #delete(E)}.
   * 
   * 3. For (c, d) in the posterior diff,
   * 3.1 (c', 0)  in this diff: replace the element in c-list --> (c, 0)
   * 3.2 (0, d')  in this diff: impossible
   * 3.3 (c', d') in this diff: replace the element in c-list --> (c, d')
   * 3.4 (0, 0)   in this diff: put in c-list and d-list --> (c, d)
   * This is the same logic as {@link #modify(E, E)}.
   * </pre>
   * 
   * @param the posterior diff to combine with.
   * @param deletedProcesser
   *     process the deleted/overwritten elements in case 2.1, 2.3, 3.1 and 3.3.
   */
  public void combinePosterior(final Diff<K, E> posterior,
      final Processor<E> deletedProcesser) {
    final Iterator<E> createdIterator = posterior.getCreatedList().iterator();
    final Iterator<E> deletedIterator = posterior.getDeletedList().iterator();

    E c = createdIterator.hasNext()? createdIterator.next(): null;
    E d = deletedIterator.hasNext()? deletedIterator.next(): null;

    for(; c != null || d != null; ) {
      final int cmp = c == null? 1
          : d == null? -1
          : c.compareTo(d.getKey());
      if (cmp < 0) {
        // case 1: only in c-list
        create(c);
        c = createdIterator.hasNext()? createdIterator.next(): null;
      } else if (cmp > 0) {
        // case 2: only in d-list
        final UndoInfo<E> ui = delete(d);
        if (deletedProcesser != null) {
          deletedProcesser.process(ui.trashed);
        }
        d = deletedIterator.hasNext()? deletedIterator.next(): null;
      } else {
        // case 3: in both c-list and d-list 
        final UndoInfo<E> ui = modify(d, c);
        if (deletedProcesser != null) {
          deletedProcesser.process(ui.trashed);
        }
        c = createdIterator.hasNext()? createdIterator.next(): null;
        d = deletedIterator.hasNext()? deletedIterator.next(): null;
      }
    }
  }

  /** Convert the element list to a compact string. */
  static <E> String toString(List<E> elements) {
    if (elements == null || elements.isEmpty()) {
      return "<empty>";
    }
    final StringBuilder b = new StringBuilder("[")
        .append(elements.get(0));
    for(int i = 1; i < elements.size(); i++) {
      b.append(", ").append(elements.get(i));
    }
    return b.append("]").toString();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "{created=" + toString(created)
        + ", deleted=" + toString(deleted) + "}";
  }
}
