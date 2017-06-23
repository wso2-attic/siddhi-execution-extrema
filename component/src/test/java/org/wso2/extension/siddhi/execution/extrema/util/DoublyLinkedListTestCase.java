/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 */

package org.wso2.extension.siddhi.execution.extrema.util;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DoublyLinkedListTestCase {
    private static final Logger log = Logger.getLogger(DoublyLinkedList.class);
    private DoublyLinkedList<String> doublyLinkedList;
    private DoublyLinkedList<String> emptyDoublyLinkedList;
    private List<String> expectedOutput;
    private List<ListNode<String>> nodes;

    @BeforeMethod
    public void init() {
        nodes = new ArrayList<ListNode<String>>();
        doublyLinkedList = new DoublyLinkedList<String>();
        emptyDoublyLinkedList = new DoublyLinkedList<String>();

        nodes.add(doublyLinkedList.addAfterLast("item1"));
        nodes.add(doublyLinkedList.addAfterLast("item2"));
        nodes.add(doublyLinkedList.addAfterLast("item3"));
        nodes.add(doublyLinkedList.addAfterLast("item4"));

        expectedOutput = new LinkedList<String>(Arrays.asList("item1", "item2", "item3", "item4"));
    }

    @Test
    public void testDoublyLinkedListForAddAfterLastWithValue() {
        log.info("DoublyLinkedList TestCase 1");

        nodes.add(doublyLinkedList.addAfterLast("newNode"));
        expectedOutput.add("newNode");
        int i = 0;
        for (ListNode<String> item = doublyLinkedList.head(); item != null; item = item.getNextNode()) {
            AssertJUnit.assertEquals(expectedOutput.get(i), item.getValue());
            i++;
        }
        AssertJUnit.assertEquals(nodes.get(nodes.size() - 1), doublyLinkedList.tail());
        AssertJUnit.assertEquals(5, doublyLinkedList.size());
    }

    @Test
    public void testDoublyLinkedListForAddAfterLastWithListNode() {
        log.info("DoublyLinkedList TestCase 2");

        ListNode<String> newNode = new ListNode<String>("newNode");
        doublyLinkedList.addAfterLast(newNode);
        nodes.add(newNode);
        expectedOutput.add("newNode");
        int i = 0;
        for (ListNode<String> item = doublyLinkedList.head(); item != null; item = item.getNextNode()) {
            AssertJUnit.assertEquals(expectedOutput.get(i), item.getValue());
            i++;
        }
        AssertJUnit.assertEquals(nodes.get(nodes.size() - 1), doublyLinkedList.tail());
        AssertJUnit.assertEquals(5, doublyLinkedList.size());
    }

    @Test
    public void testDoublyLinkedListForAddBeforeFirstWithValue() {
        log.info("DoublyLinkedList TestCase 3");

        nodes.add(0, doublyLinkedList.addBeforeFirst("newNode"));
        expectedOutput.add(0, "newNode");
        int i = 0;
        for (ListNode<String> item = doublyLinkedList.head(); item != null; item = item.getNextNode()) {
            AssertJUnit.assertEquals(expectedOutput.get(i), item.getValue());
            i++;
        }
        AssertJUnit.assertEquals(nodes.get(0), doublyLinkedList.head());
        AssertJUnit.assertEquals(5, doublyLinkedList.size());
    }

    @Test
    public void testDoublyLinkedListForAddBeforeFirstWithListNode() {
        log.info("DoublyLinkedList TestCase 4");

        ListNode<String> newNode = new ListNode<String>("newNode");
        doublyLinkedList.addBeforeFirst(newNode);
        nodes.add(0, newNode);
        expectedOutput.add(0, "newNode");
        int i = 0;
        for (ListNode<String> item = doublyLinkedList.head(); item != null; item = item.getNextNode()) {
            AssertJUnit.assertEquals(expectedOutput.get(i), item.getValue());
            i++;
        }
        AssertJUnit.assertEquals(nodes.get(0), doublyLinkedList.head());
        AssertJUnit.assertEquals(5, doublyLinkedList.size());
    }

    @Test
    public void testEmptyDoublyLinkedListForAddAfterLastWithValue() {
        log.info("DoublyLinkedList TestCase 5");

        ListNode<String> newNode = emptyDoublyLinkedList.addAfterLast("newNode");
        AssertJUnit.assertEquals(newNode, emptyDoublyLinkedList.head());
        AssertJUnit.assertEquals(newNode, emptyDoublyLinkedList.tail());
        AssertJUnit.assertEquals(1, emptyDoublyLinkedList.size());
    }

    @Test
    public void testEmptyDoublyLinkedListForAddAfterLastWithListNode() {
        log.info("DoublyLinkedList TestCase 6");

        ListNode<String> newNode = new ListNode<String>("newNode");
        emptyDoublyLinkedList.addAfterLast(newNode);
        AssertJUnit.assertEquals(newNode, emptyDoublyLinkedList.head());
        AssertJUnit.assertEquals(newNode, emptyDoublyLinkedList.tail());
        AssertJUnit.assertEquals(1, emptyDoublyLinkedList.size());
    }

    @Test
    public void testEmptyDoublyLinkedListForAddBeforeFirstWithValue() {
        log.info("DoublyLinkedList TestCase 7");

        ListNode<String> newNode = emptyDoublyLinkedList.addBeforeFirst("newNode");
        AssertJUnit.assertEquals(newNode, emptyDoublyLinkedList.head());
        AssertJUnit.assertEquals(newNode, emptyDoublyLinkedList.tail());
        AssertJUnit.assertEquals(1, emptyDoublyLinkedList.size());
    }

    @Test
    public void testEmptyDoublyLinkedListForAddBeforeFirstWithListNode() {
        log.info("DoublyLinkedList TestCase 8");

        ListNode<String> newNode = new ListNode<String>("newNode");
        emptyDoublyLinkedList.addBeforeFirst(newNode);
        AssertJUnit.assertEquals(newNode, emptyDoublyLinkedList.head());
        AssertJUnit.assertEquals(newNode, emptyDoublyLinkedList.tail());
        AssertJUnit.assertEquals(1, emptyDoublyLinkedList.size());
    }

    @Test
    public void testDoublyLinkedListForAddAfterNodeWithValue() {
        log.info("DoublyLinkedList TestCase 9");

        doublyLinkedList.addAfterNode(nodes.get(2), "newNode");
        expectedOutput.add(3, "newNode");
        int i = 0;
        for (ListNode<String> item = doublyLinkedList.head(); item != null; item = item.getNextNode()) {
            AssertJUnit.assertEquals(expectedOutput.get(i), item.getValue());
            i++;
        }
        AssertJUnit.assertEquals(5, doublyLinkedList.size());
    }

    @Test
    public void testDoublyLinkedListForAddAfterNodeWithListNode() {
        log.info("DoublyLinkedList TestCase 10");

        doublyLinkedList.addAfterNode(nodes.get(2), new ListNode<String>("newNode"));
        expectedOutput.add(3, "newNode");
        int i = 0;
        for (ListNode<String> item = doublyLinkedList.head(); item != null; item = item.getNextNode()) {
            AssertJUnit.assertEquals(expectedOutput.get(i), item.getValue());
            i++;
        }
        AssertJUnit.assertEquals(5, doublyLinkedList.size());
    }

    @Test
    public void testDoublyLinkedListForRemove() {
        log.info("DoublyLinkedList TestCase 11");

        doublyLinkedList.remove(nodes.get(2));
        expectedOutput.remove(2);
        int i = 0;
        for (ListNode<String> item = doublyLinkedList.head(); item != null; item = item.getNextNode()) {
            AssertJUnit.assertEquals(expectedOutput.get(i), item.getValue());
            i++;
        }
        AssertJUnit.assertEquals(3, doublyLinkedList.size());
    }

    @Test
    public void testDoublyLinkedListForFirstAndLast() {
        log.info("DoublyLinkedList TestCase 12");

        AssertJUnit.assertEquals(expectedOutput.get(0), doublyLinkedList.first());
        AssertJUnit.assertEquals(expectedOutput.get(expectedOutput.size() - 1), doublyLinkedList.last());
    }

    @Test
    public void testDoublyLinkedListForHeadAndTail() {
        log.info("DoublyLinkedList TestCase 13");

        AssertJUnit.assertEquals(nodes.get(0), doublyLinkedList.head());
        AssertJUnit.assertEquals(nodes.get(nodes.size() - 1), doublyLinkedList.tail());
    }

    @Test
    public void testDoublyLinkedListForIsEmpty() {
        log.info("DoublyLinkedList TestCase 14");

        AssertJUnit.assertFalse(doublyLinkedList.isEmpty());
        for (int i = 0; i < nodes.size(); i++) {
            doublyLinkedList.remove(nodes.get(i));
        }
        AssertJUnit.assertTrue(doublyLinkedList.isEmpty());
    }
}
