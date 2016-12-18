/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 *
 */

package org.wso2.extension.siddhi.execution.extrema.util;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;

import java.util.TreeMap;

/**
 * class which has the logic implementation to get the event which hold min/max value corresponding to given attribute
 */
public class MaxByMinByExecutor {
    private String minByMaxByExecutorType;
    private TreeMap<Object, StreamEvent> sortedEventMap = new TreeMap<Object, StreamEvent>();

    public TreeMap<Object, StreamEvent> getSortedEventMap() {
        return sortedEventMap;
    }

    public void setSortedEventMap(TreeMap<Object, StreamEvent> sortedEventMap) {
        this.sortedEventMap = sortedEventMap;
    }

    public String getMinByMaxByExecutorType() {
        return minByMaxByExecutorType;
    }

    public void setMinByMaxByExecutorType(String minByMaxByExecutorType) {
        this.minByMaxByExecutorType = minByMaxByExecutorType;
    }

    /**
     * To insert the current event into treemap .
     *
     * @param clonedStreamEvent copy of current event
     * @param parameterValue    key for the treemap(object which holds the parameter value)
     */
    public void insert(StreamEvent clonedStreamEvent, Object parameterValue) {
        sortedEventMap.put(parameterValue, clonedStreamEvent);
    }

    /**
     * To find the event which holds minimum or maximum  value of given parameter.
     *
     * @param functionType MIN/MAX
     * @return outputEvent
     */
    public StreamEvent getResult(String functionType) {
        StreamEvent outputEvent;
        if (functionType.equals(MaxByMinByConstants.MIN_BY)) {
            Object minEventKey = sortedEventMap.firstKey();
            outputEvent = sortedEventMap.get(minEventKey);
        } else {
            Object maxEventKey = sortedEventMap.lastKey();
            outputEvent = sortedEventMap.get(maxEventKey);
        }
        return outputEvent;
    }

    /**
     * Return the minimum event comparing two events
     *
     * @param currentEvent   new event
     * @param oldEvent       the previous event that is stored as the minimun event
     * @param minByAttribute the attribute which the comparison is done.
     * @return minEvent
     */

    public static StreamEvent getMinEventBatchProcessor(StreamEvent currentEvent, StreamEvent oldEvent,
            ExpressionExecutor minByAttribute) {
        StreamEvent minEvent = oldEvent;
        if (minEvent != null) {
            Object minEventValue = minByAttribute.execute(minEvent);
            Object currentEventValue = minByAttribute.execute(currentEvent);
            if (minEventValue instanceof Comparable && currentEventValue instanceof Comparable) {
                Comparable minValue = (Comparable) minEventValue;
                Comparable currentValue = (Comparable) currentEventValue;
                if (currentValue.compareTo(minValue) <= 0) {
                    minEvent = currentEvent;
                }
            }
        } else {
            minEvent = currentEvent;
        }
        return minEvent;
    }

    /**
     * Retrun the maximum event comparing two events
     *
     * @param currentEvent   new event
     * @param oldEvent       the previous event that is stored as the maximum event
     * @param maxByAttribute the attribute which the comparison is done.
     * @return maxEvent
     */
    public static StreamEvent getMaxEventBatchProcessor(StreamEvent currentEvent, StreamEvent oldEvent,
            ExpressionExecutor maxByAttribute) {
        StreamEvent maxEvent = oldEvent;
        if (maxEvent != null) {
            Object maxEventValue = maxByAttribute.execute(maxEvent);
            Object currentEventValue = maxByAttribute.execute(currentEvent);
            if (maxEventValue instanceof Comparable && currentEventValue instanceof Comparable) {
                Comparable maxValue = (Comparable) maxEventValue;
                Comparable currentValue = (Comparable) currentEventValue;
                if (currentValue.compareTo(maxValue) >= 0) {
                    maxEvent = currentEvent;
                }
            }
        } else {
            maxEvent = currentEvent;
        }
        return maxEvent;
    }

}
