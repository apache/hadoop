/*
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
package org.apache.hadoop.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

import com.google.common.base.Preconditions;

/**
 * Implements an intrusive doubly-linked list.
 *
 * An intrusive linked list is one in which the elements themselves are
 * responsible for storing the pointers to previous and next elements.
 * This can save a lot of memory if there are many elements in the list or
 * many lists.
 */
@InterfaceAudience.Private
public class IntrusiveCollection<E extends IntrusiveCollection.Element>
    implements Collection<E> {
  /**
   * An element contained in this list.
   *
   * We pass the list itself as a parameter so that elements can belong to
   * multiple lists.  (The element will need to store separate prev and next
   * pointers for each.)
   */
  @InterfaceAudience.Private
  public interface Element {
    /**
     * Insert this element into the list.  This is the first thing that will
     * be called on the element.
     */
    void insertInternal(IntrusiveCollection<? extends Element> list,
        Element prev, Element next);

    /**
     * Set the prev pointer of an element already in the list.
     */
    void setPrev(IntrusiveCollection<? extends Element> list, Element prev);

    /**
     * Set the next pointer of an element already in the list.
     */
    void setNext(IntrusiveCollection<? extends Element> list, Element next);

    /**
     * Remove an element from the list.  This is the last thing that will be
     * called on an element.
     */
    void removeInternal(IntrusiveCollection<? extends Element> list);

    /**
     * Get the prev pointer of an element.
     */
    Element getPrev(IntrusiveCollection<? extends Element> list);

    /**
     * Get the next pointer of an element.
     */
    Element getNext(IntrusiveCollection<? extends Element> list);

    /**
     * Returns true if this element is in the provided list.
     */
    boolean isInList(IntrusiveCollection<? extends Element> list);
  }

  private Element root = new Element() {
    // We keep references to the first and last elements for easy access.
    Element first = this;
    Element last = this;
  
    @Override
    public void insertInternal(IntrusiveCollection<? extends Element> list,
        Element prev, Element next) {
      throw new RuntimeException("Can't insert root element");
    }

    @Override
    public void setPrev(IntrusiveCollection<? extends Element> list,
        Element prev) {
      Preconditions.checkState(list == IntrusiveCollection.this);
      last = prev;
    }

    @Override
    public void setNext(IntrusiveCollection<? extends Element> list,
        Element next) {
      Preconditions.checkState(list == IntrusiveCollection.this);
      first = next;
    }
  
    @Override
    public void removeInternal(IntrusiveCollection<? extends Element> list) {
      throw new RuntimeException("Can't remove root element");
    }
    
    @Override
    public Element getNext(
        IntrusiveCollection<? extends Element> list) {
      Preconditions.checkState(list == IntrusiveCollection.this);
      return first;
    }
  
    @Override
    public Element getPrev(
        IntrusiveCollection<? extends Element> list) {
      Preconditions.checkState(list == IntrusiveCollection.this);
      return last;
    }

    @Override
    public boolean isInList(IntrusiveCollection<? extends Element> list) {
      return list == IntrusiveCollection.this;
    }

    @Override
    public String toString() {
      return "root"; // + IntrusiveCollection.this + "]";
    }
  };

  private int size = 0;

  /**
   * An iterator over the intrusive collection.
   *
   * Currently, you can remove elements from the list using
   * #{IntrusiveIterator#remove()}, but modifying the collection in other
   * ways during the iteration is not supported.
   */
  public class IntrusiveIterator implements Iterator<E> {
    Element cur;
    Element next;

    IntrusiveIterator() {
      this.cur = root;
      this.next = null;
    }

    @Override
    public boolean hasNext() {
      if (next == null) {
        next = cur.getNext(IntrusiveCollection.this);
      }
      return next != root;
    }

    @SuppressWarnings("unchecked")
    @Override
    public E next() {
      if (next == null) {
        next = cur.getNext(IntrusiveCollection.this);
      }
      if (next == root) {
        throw new NoSuchElementException();
      }
      cur = next;
      next = null;
      return (E)cur;
    }

    @Override
    public void remove() {
      if (cur == null) {
        throw new IllegalStateException("Already called remove " +
            "once on this element.");
      }
      next = removeElement(cur);
      cur = null;
    }
  }
  
  private Element removeElement(Element elem) {
    Element prev = elem.getPrev(IntrusiveCollection.this);
    Element next = elem.getNext(IntrusiveCollection.this);
    elem.removeInternal(IntrusiveCollection.this);
    prev.setNext(IntrusiveCollection.this, next);
    next.setPrev(IntrusiveCollection.this, prev);
    size--;
    return next;
  }

  /**
   * Get an iterator over the list.  This can be used to remove elements.
   * It is not safe to do concurrent modifications from other threads while
   * using this iterator.
   * 
   * @return         The iterator.
   */
  public Iterator<E> iterator() {
    return new IntrusiveIterator();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public boolean contains(Object o) {
    try {
      Element element = (Element)o;
      return element.isInList(this);
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override
  public Object[] toArray() {
    Object ret[] = new Object[size];
    int i = 0;
    for (Iterator<E> iter = iterator(); iter.hasNext(); ) {
      ret[i++] = iter.next();
    }
    return ret;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T[] toArray(T[] array) {
    if (array.length < size) {
      return (T[])toArray();
    } else {
      int i = 0;
      for (Iterator<E> iter = iterator(); iter.hasNext(); ) {
        array[i++] = (T)iter.next();
      }
    }
    return array;
  }

  /**
   * Add an element to the end of the list.
   * 
   * @param elem     The new element to add.
   */
  @Override
  public boolean add(E elem) {
    if (elem == null) {
      return false;
    }
    if (elem.isInList(this)) {
      return false;
    }
    Element prev = root.getPrev(IntrusiveCollection.this);
    prev.setNext(IntrusiveCollection.this, elem);
    root.setPrev(IntrusiveCollection.this, elem);
    elem.insertInternal(IntrusiveCollection.this, prev, root);
    size++;
    return true;
  }

  /**
   * Add an element to the front of the list.
   *
   * @param elem     The new element to add.
   */
  public boolean addFirst(Element elem) {
    if (elem == null) {
      return false;
    }
    if (elem.isInList(this)) {
      return false;
    }
    Element next = root.getNext(IntrusiveCollection.this);
    next.setPrev(IntrusiveCollection.this, elem);
    root.setNext(IntrusiveCollection.this, elem);
    elem.insertInternal(IntrusiveCollection.this, root, next);
    size++;
    return true;
  }

  public static final Log LOG = LogFactory.getLog(IntrusiveCollection.class);

  @Override
  public boolean remove(Object o) {
    try {
      Element elem = (Element)o;
      if (!elem.isInList(this)) {
        return false;
      }
      removeElement(elem);
      return true;
    } catch (ClassCastException e) {
      return false;
    }
  }

  @Override
  public boolean containsAll(Collection<?> collection) {
    for (Object o : collection) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends E> collection) {
    boolean changed = false;
    for (E elem : collection) {
      if (add(elem)) {
        changed = true;
      }
    }
    return changed;
  }

  @Override
  public boolean removeAll(Collection<?> collection) {
    boolean changed = false;
    for (Object elem : collection) {
      if (remove(elem)) {
        changed = true;
      }
    }
    return changed;
  }

  @Override
  public boolean retainAll(Collection<?> collection) {
    boolean changed = false;
    for (Iterator<E> iter = iterator();
        iter.hasNext(); ) {
      Element elem = iter.next();
      if (!collection.contains(elem)) {
        iter.remove();
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Remove all elements.
   */
  @Override
  public void clear() {
    for (Iterator<E> iter = iterator(); iter.hasNext(); ) {
      iter.next();
      iter.remove();
    }
  }
}
