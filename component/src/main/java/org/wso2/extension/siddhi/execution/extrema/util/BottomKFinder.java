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

import java.util.ArrayList;
import java.util.List;

/**
 * Class for finding bottom K items and their frequencies.
 * @param <T> Type of items
 */
public class BottomKFinder<T> extends AbstractTopKBottomKFinder<T> {
    /**
     * Sets the capacity as Integer max
     */
    public BottomKFinder() {
        super(Integer.MAX_VALUE);
    }

    /**
     * @param capacity Maximum number of unique items to keep
     */
    public BottomKFinder(int capacity) {
        super(capacity);
    }

    /**
     * Get the bottomK items and their frequencies
     */
    @Override
    public List<Counter<T>> get(int k) {
        List<Counter<T>> bottomK = new ArrayList<Counter<T>>(k);
        for (ListNode<Bucket> bNode = bucketList.head(); bNode != null; bNode = bNode.getNextNode()) {
            Bucket b = bNode.getValue();
            for (ListNode<Counter<T>> cNode = b.getCounterList().tail();
                 cNode != null;
                 cNode = cNode.getPreviousNode()) {
                bottomK.add(cNode.getValue());
                if (bottomK.size() == k) {
                    return bottomK;
                }
            }
        }
        return bottomK;
    }

    /**
     * Get the bucket with the counter that should be replaced
     * The least important counter will be considered
     * The counter considered here is the counter with the highest frequency
     */
    @Override
    protected Bucket getBucketWithCounterToReplace() {
        return bucketList.last();
    }
}
