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

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for TopKTimeBatchStreamProcessorExtension extension.
 */
public class TopKTimeBatchStreamProcessorExtensionTestCase {
    private static final Logger log = Logger.getLogger(
            TopKTimeBatchStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;
    private AtomicInteger eventCount;
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void init() {
        count = 0;
        eventCount = new AtomicInteger(0);
        eventArrived = false;
    }

    @Test
    public void testTopKTimeBatchStreamProcessorExtensionWithoutStartTime() throws InterruptedException {
        log.info("TopKTimeBatchStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#extrema:topKTimeBatch(item, 1 sec, 3)  " +
                "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        addQueryCallbackForSimpleQuery(siddhiAppRuntime);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"item1", 10L});
        inputHandler.send(new Object[]{"item1", 13L});
        inputHandler.send(new Object[]{"item2", 65L});
        inputHandler.send(new Object[]{"item1", 74L});
        inputHandler.send(new Object[]{"item2", 25L});
        inputHandler.send(new Object[]{"item3", 64L});
        // Time Window reset
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"item1", 10L});
        inputHandler.send(new Object[]{"item1", 13L});
        inputHandler.send(new Object[]{"item2", 65L});
        inputHandler.send(new Object[]{"item1", 74L});
        inputHandler.send(new Object[]{"item2", 25L});
        inputHandler.send(new Object[]{"item3", 64L});
        // Time Window reset
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"item4", 75L});
        inputHandler.send(new Object[]{"item4", 34L});

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        AssertJUnit.assertEquals(4, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTopKTimeBatchStreamProcessorExtensionWithStartTime() throws InterruptedException {
        log.info("TopKTimeBatchStreamProcessor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#extrema:topKTimeBatch(item, 1 sec, 3, 1000)  " +
                "insert all events into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        addQueryCallbackForSimpleQuery(siddhiAppRuntime);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{"item3", 43L});
        inputHandler.send(new Object[]{"item3", 61L});
        inputHandler.send(new Object[]{"item3", 44L});
        // Start time
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"item1", 10L});
        inputHandler.send(new Object[]{"item1", 13L});
        inputHandler.send(new Object[]{"item2", 65L});
        inputHandler.send(new Object[]{"item1", 74L});
        inputHandler.send(new Object[]{"item2", 25L});
        inputHandler.send(new Object[]{"item3", 64L});
        // Time Window reset
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"item1", 10L});
        inputHandler.send(new Object[]{"item1", 13L});
        inputHandler.send(new Object[]{"item2", 65L});
        inputHandler.send(new Object[]{"item1", 74L});
        inputHandler.send(new Object[]{"item2", 25L});
        inputHandler.send(new Object[]{"item3", 64L});
        // Time Window reset
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"item4", 75L});
        inputHandler.send(new Object[]{"item4", 34L});

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        AssertJUnit.assertEquals(4, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTopKTimeBatchStreamProcessorExtensionWithJoin() throws InterruptedException {
        log.info("TopKTimeBatchStreamProcessor TestCase 3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream1 (item string, price long);" +
                "define stream inputStream2 (item string, type string);";
        String query = ("@info(name = 'query1') " +
                "from inputStream1#extrema:topKTimeBatch(item, 1 sec, 3) as stream1 " +
                "join inputStream2#window.timeBatch(1 sec) as stream2 " +
                "on stream1.Top1Element==stream2.item " +
                "select stream2.item as item, stream2.type as type " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                AssertJUnit.assertNotNull(events);
                if (events.length < 1) {
                    return;
                }
                eventArrived = true;
                for (Event event : events) {
                    if (count == 0) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item1", "cash"),
                                Arrays.asList(event.getData())
                        );
                    } else if (count == 1) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item6", "cash"),
                                Arrays.asList(event.getData())
                        );
                    }
                }
                eventCount.incrementAndGet();
                count++;
            }
        });

        InputHandler inputHandler1 = siddhiAppRuntime.getInputHandler("inputStream1");
        InputHandler inputHandler2 = siddhiAppRuntime.getInputHandler("inputStream2");
        siddhiAppRuntime.start();

        inputHandler1.send(new Object[]{"item1", 10L});
        inputHandler1.send(new Object[]{"item1", 13L});
        inputHandler1.send(new Object[]{"item2", 65L});
        inputHandler1.send(new Object[]{"item1", 74L});
        inputHandler1.send(new Object[]{"item2", 25L});
        inputHandler1.send(new Object[]{"item3", 64L});
        inputHandler2.send(new Object[]{"item1", "cash"});
        inputHandler2.send(new Object[]{"item2", "credit card"});
        inputHandler2.send(new Object[]{"item3", "voucher"});
        // Time Window reset
        Thread.sleep(1100);
        inputHandler1.send(new Object[]{"item4", 65L});
        inputHandler1.send(new Object[]{"item5", 45L});
        inputHandler1.send(new Object[]{"item6", 34L});
        inputHandler1.send(new Object[]{"item4", 76L});
        inputHandler1.send(new Object[]{"item5", 93L});
        inputHandler1.send(new Object[]{"item6", 23L});
        inputHandler2.send(new Object[]{"item5", "voucher"});
        inputHandler2.send(new Object[]{"item4", "credit card"});
        inputHandler2.send(new Object[]{"item6", "cash"});

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    private void addQueryCallbackForSimpleQuery(SiddhiAppRuntime siddhiAppRuntime) {
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
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
                    AssertJUnit.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item3", 64L, "item1", 3L, "item2", 2L, "item3", 1L),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertTrue(event.isExpired());
                    }
                } else if (count == 2) {
                    AssertJUnit.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item4", 34L, "item4", 2L, null, null, null, null),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertFalse(event.isExpired());
                    }
                    AssertJUnit.assertNull(removeEvents);
                } else if (count == 2) {
                    AssertJUnit.assertNull(inEvents);
                    AssertJUnit.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        AssertJUnit.assertEquals(
                                Arrays.asList("item4", 34L, "item4", 2L, null, null, null, null),
                                Arrays.asList(event.getData())
                        );
                        AssertJUnit.assertTrue(event.isExpired());
                    }
                }
                count++;
                eventCount.incrementAndGet();
            }
        });
    }
}
