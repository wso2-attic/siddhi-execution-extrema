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

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TopKFinderTestCase {
    private static final Logger log = Logger.getLogger(TopKFinder.class);
    AbstractTopKBottomKFinder<String> topKFinder;

    @Before
    public void init() {
        topKFinder = new TopKFinder<String>();
    }

    @Test
    public void testTopKLengthBatchStreamProcessorExtension() {
        log.info("TopKFinder TestCase 1");

        topKFinder.offer("item1");
        topKFinder.offer("item1");
        topKFinder.offer("item1");
        topKFinder.offer("item1");
        topKFinder.offer("item2");
        topKFinder.offer("item2");
        topKFinder.offer("item2");
        topKFinder.offer("item3");
        topKFinder.offer("item3");
        topKFinder.offer("item4");
        topKFinder.offer("item5");

        List<Counter<String>> counters = topKFinder.get(3);
        Assert.assertEquals(3, counters.size());

        Assert.assertEquals("item1", counters.get(0).getItem());
        Assert.assertEquals(4, counters.get(0).getCount());
        Assert.assertEquals("item2", counters.get(1).getItem());
        Assert.assertEquals(3, counters.get(1).getCount());
        Assert.assertEquals("item3", counters.get(2).getItem());
        Assert.assertEquals(2, counters.get(2).getCount());

        topKFinder.offer("item1", -1);
        topKFinder.offer("item2", -1);
        topKFinder.offer("item3", -2);

        counters = topKFinder.get(3);
        Assert.assertEquals(3, counters.size());

        Assert.assertEquals("item1", counters.get(0).getItem());
        Assert.assertEquals(3, counters.get(0).getCount());
        Assert.assertEquals("item2", counters.get(1).getItem());
        Assert.assertEquals(2, counters.get(1).getCount());
        Assert.assertEquals("item4", counters.get(2).getItem());
        Assert.assertEquals(1, counters.get(2).getCount());

        topKFinder.offer("item4", -1);
        topKFinder.offer("item5", -1);

        counters = topKFinder.get(3);
        Assert.assertEquals(2, counters.size());

        Assert.assertEquals("item1", counters.get(0).getItem());
        Assert.assertEquals(3, counters.get(0).getCount());
        Assert.assertEquals("item2", counters.get(1).getItem());
        Assert.assertEquals(2, counters.get(1).getCount());
    }
}
