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

package org.apache.hadoop.fs.azure;

/**
 * A simple generic stack implementation using linked lists. The stack
 * implementation has five main operations:
 * <ul>
 * <li>push -- adds an element to the top of the stack</li>
 * <li>pop -- removes an element from the top of the stack and returns a
 * reference to it</li>
 * <li>peek -- peek returns an element from the top of the stack without
 * removing it</li>
 * <li>isEmpty -- tests whether the stack is empty</li>
 * <li>size -- returns the size of the stack</li>
 * <li>toString -- returns a string representation of the stack.</li>
 * </ul>
 */

public class AzureLinkedStack<E> {
  /*
   * Linked node for Azure stack collection.
   */
  private static class AzureLinkedNode<E> {
    private E element; // Linked element on the list.
    private AzureLinkedNode<E> next;// Reference to the next linked element on
                                    // list.

    /*
     * The constructor builds the linked node with no successor
     *
     * @param element : The value of the element to be stored with this node.
     */
    private AzureLinkedNode(E anElement) {
      element = anElement;
      next = null;
    }

    /*
     * Constructor builds a linked node with a specified successor. The
     * successor may be null.
     *
     * @param anElement : new element to be created.
     *
     * @param nextElement: successor to the new element.
     */
    private AzureLinkedNode(E anElement, AzureLinkedNode<E> nextElement) {
      element = anElement;
      next = nextElement;
    }

    /*
     * Get the element stored in the linked node.
     *
     * @return E : element stored in linked node.
     */
    private E getElement() {
      return element;
    }

    /*
     * Get the successor node to the element.
     *
     * @return E : reference to the succeeding node on the list.
     */
    private AzureLinkedNode<E> getNext() {
      return next;
    }
  }

  private int count; // The number of elements stored on the stack.
  private AzureLinkedNode<E> top; // Top of the stack.

  /*
   * Constructor creating an empty stack.
   */
  public AzureLinkedStack() {
    // Simply initialize the member variables.
    //
    count = 0;
    top = null;
  }

  /*
   * Adds an element to the top of the stack.
   *
   * @param element : element pushed to the top of the stack.
   */
  public void push(E element) {
    // Create a new node containing a reference to be placed on the stack.
    // Set the next reference to the new node to point to the current top
    // of the stack. Set the top reference to point to the new node. Finally
    // increment the count of nodes on the stack.
    //
    AzureLinkedNode<E> newNode = new AzureLinkedNode<E>(element, top);
    top = newNode;
    count++;
  }

  /*
   * Removes the element at the top of the stack and returns a reference to it.
   *
   * @return E : element popped from the top of the stack.
   *
   * @throws Exception on pop from an empty stack.
   */
  public E pop() throws Exception {
    // Make sure the stack is not empty. If it is empty, throw a StackEmpty
    // exception.
    //
    if (isEmpty()) {
      throw new Exception("AzureStackEmpty");
    }

    // Set a temporary reference equal to the element at the top of the stack,
    // decrement the count of elements and return reference to the temporary.
    //
    E element = top.getElement();
    top = top.getNext();
    count--;

    // Return the reference to the element that was at the top of the stack.
    //
    return element;
  }

  /*
   * Return the top element of the stack without removing it.
   *
   * @return E
   *
   * @throws Exception on peek into an empty stack.
   */
  public E peek() throws Exception {
    // Make sure the stack is not empty. If it is empty, throw a StackEmpty
    // exception.
    //
    if (isEmpty()) {
      throw new Exception("AzureStackEmpty");
    }

    // Set a temporary reference equal to the element at the top of the stack
    // and return the temporary.
    //
    E element = top.getElement();
    return element;
  }

  /*
   * Determines whether the stack is empty
   *
   * @return boolean true if the stack is empty and false otherwise.
   */
  public boolean isEmpty() {
    if (0 == size()) {
      // Zero-sized stack so the stack is empty.
      //
      return true;
    }

    // The stack is not empty.
    //
    return false;
  }

  /*
   * Determines the size of the stack
   *
   * @return int: Count of the number of elements in the stack.
   */
  public int size() {
    return count;
  }

  /*
   * Returns a string representation of the stack.
   *
   * @return String String representation of all elements in the stack.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    AzureLinkedNode<E> current = top;
    for (int i = 0; i < size(); i++) {
      E element = current.getElement();
      sb.append(element.toString());
      current = current.getNext();

      // Insert commas between strings except after the last string.
      //
      if (size() - 1 > i) {
        sb.append(", ");
      }
    }

    // Return the string.
    //
    return sb.toString();
  }
}
