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
 *
 */
package org.wso2.extension.siddhi.execution.extrema.util;

/**
 * Used for storing the item value and the frequency
 * Used in TopK/BottomK extensions
 *
 * @param <T> The type of item to store
 */
public class Counter<T> {
    private ListNode<AbstractTopKBottomKFinder<T>.Bucket> bucketNode;

    private T item;
    private long count;

    public Counter(ListNode<AbstractTopKBottomKFinder<T>.Bucket> bucket, T item) {
        bucketNode = bucket;
        count = 0;
        this.item = item;
    }

    public ListNode<AbstractTopKBottomKFinder<T>.Bucket> getBucketNode() {
        return bucketNode;
    }

    public void setBucketNode(ListNode<AbstractTopKBottomKFinder<T>.Bucket> bucketNode) {
        this.bucketNode = bucketNode;
    }

    public void increaseCount(long increaseAmount) {
        count += increaseAmount;
    }

    public T getItem() {
        return item;
    }

    public void setItem(T item) {
        this.item = item;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
