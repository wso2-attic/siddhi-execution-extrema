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

public class BottomKFinderTestCase {
    private static final Logger log = Logger.getLogger(BottomKFinderTestCase.class);
    AbstractTopKBottomKFinder<String> bottomKFinder;

    @Before
    public void init() {
        bottomKFinder = new BottomKFinder<String>();
    }

    @Test
    public void testTopKLengthBatchStreamProcessorExtension() {
        log.info("BottomKFinder TestCase 1");

        bottomKFinder.offer("item1");
        bottomKFinder.offer("item1");
        bottomKFinder.offer("item1");
        bottomKFinder.offer("item2");
        bottomKFinder.offer("item2");
        bottomKFinder.offer("item3");
        bottomKFinder.offer("item4");
        bottomKFinder.offer("item4");
        bottomKFinder.offer("item4");
        bottomKFinder.offer("item4");
        bottomKFinder.offer("item5");
        bottomKFinder.offer("item5");
        bottomKFinder.offer("item5");
        bottomKFinder.offer("item5");

        List<Counter<String>> counters = bottomKFinder.get(3);
        Assert.assertEquals(3, counters.size());

        Assert.assertEquals("item3", counters.get(0).getItem());
        Assert.assertEquals(1, counters.get(0).getCount());
        Assert.assertEquals("item2", counters.get(1).getItem());
        Assert.assertEquals(2, counters.get(1).getCount());
        Assert.assertEquals("item1", counters.get(2).getItem());
        Assert.assertEquals(3, counters.get(2).getCount());

        bottomKFinder.offer("item3", -1);

        counters = bottomKFinder.get(3);
        Assert.assertEquals(3, counters.size());

        Assert.assertEquals("item2", counters.get(0).getItem());
        Assert.assertEquals(2, counters.get(0).getCount());
        Assert.assertEquals("item1", counters.get(1).getItem());
        Assert.assertEquals(3, counters.get(1).getCount());
        Assert.assertEquals("item4", counters.get(2).getItem());
        Assert.assertEquals(4, counters.get(2).getCount());

        bottomKFinder.offer("item4", -4);
        bottomKFinder.offer("item5", -4);

        counters = bottomKFinder.get(3);
        Assert.assertEquals(2, counters.size());

        Assert.assertEquals("item2", counters.get(0).getItem());
        Assert.assertEquals(2, counters.get(0).getCount());
        Assert.assertEquals("item1", counters.get(1).getItem());
        Assert.assertEquals(3, counters.get(1).getCount());
    }
}
