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

import java.util.HashMap;
import java.util.List;

public abstract class AbstractTopKBottomKFinder<T> {
    protected DoublyLinkedList<Bucket> bucketList;
    private int capacity;
    private HashMap<T, ListNode<Counter<T>>> counterMap;

    /**
     * @param capacity Maximum number of unique items to keep
     */
    public AbstractTopKBottomKFinder(int capacity) {
        if (capacity > 0) {
            this.capacity = capacity;
        } else {
            this.capacity = Integer.MAX_VALUE;
        }
        counterMap = new HashMap<T, ListNode<Counter<T>>>();
        bucketList = new DoublyLinkedList<Bucket>();
    }

    /**
     * Increases the frequency of the item by 1
     *
     * @param item Item to increase the frequency of
     */
    public void offer(T item) {
        offer(item, 1);
    }

    /**
     * Increases the frequency of the item by the specified amount
     *
     * @param item Item to increase the frequency of
     */
    public void offer(T item, int incrementCount) {
        if (incrementCount == 0) {
            return;
        }
        ListNode<Counter<T>> counterNode = counterMap.get(item);
        if (counterNode == null) {
            // New counter needs to be added
            if (counterMap.size() < capacity) {
                // New bucket needs to be created
                counterNode = bucketList.addBeforeFirst(new Bucket(0))
                        .getValue().getCounterList()
                        .addAfterLast(new Counter<T>(bucketList.head(), item));
            } else {
                // Least important counter needs to be removed (Importance depends on whether it is topK or bottomK)
                // Then new bucket needs to be added
                Bucket bucketWithCounterToReplace = getBucketWithCounterToReplace();
                counterNode = bucketWithCounterToReplace.getCounterList().tail();
                Counter<T> counter = counterNode.getValue();
                counterMap.remove(counter.getItem());
                counter.setItem(item);
                counter.setError(bucketWithCounterToReplace.getCount());
            }
            counterMap.put(item, counterNode);
        }
        incrementCounter(counterNode, incrementCount);
    }

    /**
     * Increase the frequency for the node specified and moves the counter to relevant bucket
     *
     * @param counterNode    The counter node to increase the frequency of
     * @param incrementCount The increment size
     */
    private void incrementCounter(ListNode<Counter<T>> counterNode, int incrementCount) {
        // Increase the counter count
        Counter<T> counter = counterNode.getValue();
        counter.increaseCount(incrementCount);

        // Detaching the counter from the old bucket node
        ListNode<Bucket> oldBucketNode = counter.getBucketNode();
        Bucket oldBucket = oldBucketNode.getValue();
        oldBucket.getCounterList().remove(counterNode);

        if (counter.getCount() == 0) {
            // Removing counter if count is 0
            counterMap.remove(counter.getItem());
        } else {
            // Finding the right bucket for the counter
            // This may or may not be an adjacent bucket because incrementCount can be aby value
            ListNode<Bucket> currentNode = oldBucketNode;
            ListNode<Bucket> nextBucketNode;
            if (incrementCount > 0) {
                nextBucketNode = oldBucketNode.getNextNode();
            } else {
                nextBucketNode = oldBucketNode.getPreviousNode();
            }
            while (nextBucketNode != null) {
                Bucket nextBucket = nextBucketNode.getValue();
                if (counter.getCount() == nextBucket.getCount()) {
                    nextBucket.getCounterList().addAfterLast(counterNode);
                    break;
                } else if (incrementCount > 0 && counter.getCount() > nextBucket.getCount()) {
                    currentNode = nextBucketNode;
                    nextBucketNode = nextBucketNode.getNextNode();
                } else if (incrementCount < 0 && counter.getCount() < nextBucket.getCount()) {
                    currentNode = nextBucketNode;
                    nextBucketNode = nextBucketNode.getPreviousNode();
                } else {
                    // New bucket node needs to be created
                    nextBucketNode = null;
                }
            }

            if (nextBucketNode == null) {
                Bucket newBucket = new Bucket(counter.getCount());
                newBucket.counterList.addAfterLast(counterNode);
                nextBucketNode = bucketList.addAfterNode(currentNode, newBucket);
            }
            counter.setBucketNode(nextBucketNode);
        }

        // Cleaning up
        if (oldBucket.counterList.isEmpty()) {
            bucketList.remove(oldBucketNode);
        }
    }

    /**
     * Get results from the current data structure
     * Results depends on whether topK or bottomK is required
     *
     * @param k The number of elements to fetch
     * @return The elements required
     */
    public abstract List<Counter<T>> get(int k);

    /**
     * Returns the bucket with the counter with least importance
     *
     * @return Bucket with the counter with least importance
     */
    protected abstract Bucket getBucketWithCounterToReplace();

    /**
     * Bucket class
     * <p>
     * Keeps a list of counter with the same of number of frequencies
     */
    public class Bucket {
        private long count;
        private DoublyLinkedList<Counter<T>> counterList;

        /**
         * @param count The frequency of the items in the bucket
         */
        public Bucket(long count) {
            this.count = count;
            counterList = new DoublyLinkedList<Counter<T>>();
        }

        public DoublyLinkedList<Counter<T>> getCounterList() {
            return counterList;
        }

        public long getCount() {
            return count;
        }
    }
}
