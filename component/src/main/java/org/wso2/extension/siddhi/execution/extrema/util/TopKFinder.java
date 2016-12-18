/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class TopKFinder<T> extends AbstractTopKBottomKFinder<T> {
    /**
     * Sets the capacity as Integer max
     */
    public TopKFinder() {
        super(Integer.MAX_VALUE);
    }

    /**
     * @param capacity Maximum number of unique items to keep
     */
    public TopKFinder(int capacity) {
        super(capacity);
    }

    /**
     * Get the topK items and their frequencies
     */
    @Override
    public List<Counter<T>> get(int k) {
        List<Counter<T>> topK = new ArrayList<Counter<T>>(k);
        for (ListNode<Bucket> bNode = bucketList.tail(); bNode != null; bNode = bNode.getPreviousNode()) {
            Bucket b = bNode.getValue();
            for (Counter<T> c : b.getCounterList()) {
                if (topK.size() == k) {
                    return topK;
                }
                topK.add(c);
            }
        }
        return topK;
    }

    /**
     * Get the bucket with the counter that should be replaced
     * The least important counter will be considered
     * The counter considered here is the counter with the lowest frequency
     */
    @Override
    protected Bucket getBucketWithCounterToReplace() {
        return bucketList.first();
    }
}
