/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.extension.siddhi.execution.extrema.util;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * A double linked list implementation
 *
 * @param <T> The type of values to be stored in the nodes of the list
 */
public class DoublyLinkedList<T> implements Iterable<T> {
    private int size;
    private ListNode<T> head;
    private ListNode<T> tail;

    /**
     * Add a node with value after the last node in the list
     *
     * @param value The value of the node to be added as the last node of the list
     */
    public ListNode<T> addAfterLast(T value) {
        ListNode<T> node = new ListNode<T>(value);
        addAfterLast(node);
        return node;
    }

    /**
     * Add a node after the last node in the list
     *
     * @param node The node to be added as the last node of the list
     */
    public void addAfterLast(ListNode<T> node) {
        node.setPreviousNode(tail);
        node.setNextNode(null);
        if (size == 0) {
            head = node;
        } else {
            tail.setNextNode(node);
        }
        tail = node;
        size++;
    }

    /**
     * Add a node with value before the first node in the list
     *
     * @param value The value of the node to be added as the first node of the list
     */
    public ListNode<T> addBeforeFirst(T value) {
        ListNode<T> node = new ListNode<T>(value);
        addBeforeFirst(node);
        return node;
    }

    /**
     * Add a node before the first node in the list
     *
     * @param node The node to be added as the first node of the list
     */
    public void addBeforeFirst(ListNode<T> node) {
        node.setNextNode(head);
        node.setPreviousNode(null);
        if (size == 0) {
            tail = node;
        } else {
            head.setPreviousNode(node);
        }
        head = node;
        size++;
    }

    /**
     * Add a node with value after the node specified
     *
     * @param node  The node after which the new node should be added
     * @param value The value of the node to be added
     * @return
     */
    public ListNode<T> addAfterNode(ListNode<T> node, T value) {
        ListNode<T> newNode = new ListNode<T>(value);
        addAfterNode(node, newNode);
        return newNode;
    }

    /**
     * Add a node  after the node specified
     *
     * @param node    The node after which the new node should be added
     * @param newNode The node to be added
     * @return
     */
    public void addAfterNode(ListNode<T> node, ListNode<T> newNode) {
        newNode.setNextNode(node.getNextNode());
        newNode.setPreviousNode(node);
        node.setNextNode(newNode);
        if (newNode.getNextNode() == null) {
            tail = newNode;
        } else {
            newNode.getNextNode().setPreviousNode(newNode);
        }
        size++;
    }

    /**
     * Remove the specified node from the list
     *
     * @param node The node to be removed
     */
    public void remove(ListNode<T> node) {
        if (node == head) {
            head = node.getNextNode();
        } else {
            node.getPreviousNode().setNextNode(node.getNextNode());
        }
        if (node == tail) {
            tail = node.getPreviousNode();
        } else {
            node.getNextNode().setPreviousNode(node.getPreviousNode());
        }
        size--;
    }

    /**
     * Return the size of the doubly linked list
     *
     * @return The size of the doubly linked list
     */
    public int size() {
        return size;
    }

    /**
     * Return the value of the head of the doubly linked list
     *
     * @return Value of the head of the doubly linked list
     */
    public T first() {
        return head == null ? null : head.getValue();
    }

    /**
     * Return the value of the tail of the doubly linked list
     *
     * @return Value of the tail of the doubly linked list
     */
    public T last() {
        return tail == null ? null : tail.getValue();
    }

    /**
     * Return the head of the doubly linked list
     *
     * @return Head of the doubly linked list
     */
    public ListNode<T> head() {
        return head;
    }

    /**
     * Return the tail of the doubly linked list
     *
     * @return Tail of the doubly linked list
     */
    public ListNode<T> tail() {
        return tail;
    }

    /**
     * Return a boolean indicating whether the list is empty
     *
     * @return True if list is empty, false if it is not empty
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Return an iterator that can be used to iterate the doubly linked list
     *
     * @return True if list is empty, false if it is not empty
     */
    @Override
    public Iterator<T> iterator() {
        return new DoublyLinkedListIterator(this);
    }

    /**
     * Iterator used for iterating the doubly linked list
     * Only used for iterating the list when retrieving the topK or bottomK values
     */
    protected class DoublyLinkedListIterator implements Iterator<T> {
        private DoublyLinkedList<T> list;
        private ListNode<T> nextItrNode;
        private int length;

        public DoublyLinkedListIterator(DoublyLinkedList<T> list) {
            length = list.size;
            this.list = list;
            nextItrNode = list.head;
        }

        /**
         * Return a boolean indicating whether a node exist after the current node
         *
         * @return Next value
         */
        @Override
        public boolean hasNext() {
            return nextItrNode != null;
        }

        /**
         * Get the next value in the iterator
         *
         * @return Next value
         */
        @Override
        public T next() {
            if (length != list.size) {
                throw new ConcurrentModificationException();
            }
            T next = nextItrNode.getValue();
            nextItrNode = nextItrNode.getNextNode();
            return next;
        }

        /**
         * Remove the current item in the iterator
         * This method is unsupported since removing is done using the iterator
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
