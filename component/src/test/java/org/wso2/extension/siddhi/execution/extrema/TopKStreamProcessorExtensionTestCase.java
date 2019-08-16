/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org)
 * All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.execution.extrema;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for TopKStreamProcessorExtension extension.
 */
public class TopKStreamProcessorExtensionTestCase {
    private static final Logger log = Logger.getLogger(TopKStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;
    private AtomicInteger eventCount;
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
        eventCount  = new AtomicInteger(0);
    }

    @Test
    public void testTopKStreamProcessorExtensionWithLengthBatchAndCurrentEventsOnly()
            throws InterruptedException {
        log.info("TopKStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#window.lengthBatch(6)#extrema:topK(item, 3)  " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                eventCount.incrementAndGet();
                if (count == 0) {
                    AssertJUnit.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item3", 64L, "item1", 3L, "item2", 2L, "item3", 1L),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertFalse(event.isExpired());
                    }
                    AssertJUnit.assertNull(removeEvents);
                } else if (count == 1) {
                    AssertJUnit.assertNull(inEvents);
                    AssertJUnit.assertNull(removeEvents);
                } else if (count == 2) {
                    AssertJUnit.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item6", 87L, "item6", 2L, "item5", 2L, "item4", 2L),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertFalse(event.isExpired());
                    }
                    AssertJUnit.assertNull(removeEvents);
                }
                count++;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"item1", 10L});
        inputHandler.send(new Object[]{"item1", 13L});
        inputHandler.send(new Object[]{"item2", 65L});
        inputHandler.send(new Object[]{"item1", 74L});
        inputHandler.send(new Object[]{"item2", 25L});
        inputHandler.send(new Object[]{"item3", 64L});
        // Length Window reset
        inputHandler.send(new Object[]{"item1", 10L});
        inputHandler.send(new Object[]{"item1", 13L});
        inputHandler.send(new Object[]{"item2", 65L});
        inputHandler.send(new Object[]{"item1", 74L});
        inputHandler.send(new Object[]{"item2", 25L});
        inputHandler.send(new Object[]{"item3", 64L});
        // Length Window reset
        inputHandler.send(new Object[]{"item4", 65L});
        inputHandler.send(new Object[]{"item5", 45L});
        inputHandler.send(new Object[]{"item6", 34L});
        inputHandler.send(new Object[]{"item4", 76L});
        inputHandler.send(new Object[]{"item5", 93L});
        inputHandler.send(new Object[]{"item6", 87L});

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTopKStreamProcessorExtensionWithTimeWindow() throws InterruptedException {
        log.info("BottomKStreamProcessor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#window.time(1 sec)#extrema:topK(item, 3) " +
                "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                eventCount.incrementAndGet();
                if (count == 2) {
                    AssertJUnit.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item1", 10L, "item1", 2L, "item2", 1L, null, null),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertFalse(event.isExpired());
                    }
                    AssertJUnit.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item1", 10L, "item1", 1L, "item2", 1L, null, null),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertTrue(event.isExpired());
                    }
                } else if (count == 8) {
                    AssertJUnit.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item6", 58L, "item6", 1L, "item5", 1L, "item4", 1L),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertFalse(event.isExpired());
                    }
                    AssertJUnit.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item5", 45L, "item5", 1L, "item4", 1L, null, null),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertTrue(event.isExpired());
                    }
                }
                count++;
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"item2", 65L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"item1", 10L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"item1", 10L});
        // Time Window reset
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"item4", 65L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"item5", 45L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"item6", 58L});

        SiddhiTestHelper.waitForEvents(waitTime, 12, eventCount, timeout);
        AssertJUnit.assertEquals(12, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
