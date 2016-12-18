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

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class TopKTimeBatchStreamProcessorExtensionTestCase {
    private static final Logger log = Logger.getLogger(TopKTimeBatchStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testTopKTimeBatchStreamProcessorExtensionWithoutStartTime() throws InterruptedException {
        log.info("TopKTimeBatchStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#extrema:topKTimeBatch(item, 1 sec, 3)  " +
                "insert all events into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        addQueryCallbackForSimpleQuery(executionPlanRuntime);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

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
        inputHandler.send(new Object[]{"item4", 14L});
        inputHandler.send(new Object[]{"item4", 73L});
        // To get all the expired events
        Thread.sleep(1100);

        Thread.sleep(1000);
        Assert.assertEquals(4, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testTopKTimeBatchStreamProcessorExtensionWithStartTime() throws InterruptedException {
        log.info("TopKTimeBatchStreamProcessor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#extrema:topKTimeBatch(item, 1 sec, 3, 1000)  " +
                "insert all events into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        addQueryCallbackForSimpleQuery(executionPlanRuntime);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

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
        // To get all the expired events
        Thread.sleep(1100);

        Thread.sleep(1000);
        Assert.assertEquals(4, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
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
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                Assert.assertNotNull(inEvents);
                for (Event event : inEvents) {
                    if (count == 0) {
                        Assert.assertEquals("item1", event.getData(0));
                        Assert.assertEquals("cash", event.getData(1));
                        Assert.assertFalse(event.isExpired());
                    } else if (count == 1) {
                        Assert.assertEquals("item4", event.getData(0));
                        Assert.assertEquals("credit card", event.getData(1));
                        Assert.assertFalse(event.isExpired());
                    }
                }
                Assert.assertNull(removeEvents);
                count++;
            }
        });

        InputHandler inputHandler1 = executionPlanRuntime.getInputHandler("inputStream1");
        InputHandler inputHandler2 = executionPlanRuntime.getInputHandler("inputStream2");
        executionPlanRuntime.start();

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

        Thread.sleep(1100);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    private void addQueryCallbackForSimpleQuery(ExecutionPlanRuntime executionPlanRuntime) {
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (count == 0) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals("item1", event.getData(2));
                        Assert.assertEquals(3L, event.getData(3));
                        Assert.assertEquals("item2", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item3", event.getData(6));
                        Assert.assertEquals(1L, event.getData(7));
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNull(removeEvents);
                } else if (count == 1) {
                    Assert.assertNull(inEvents);
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals("item1", event.getData(2));
                        Assert.assertEquals(3L, event.getData(3));
                        Assert.assertEquals("item2", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item3", event.getData(6));
                        Assert.assertEquals(1L, event.getData(7));
                        Assert.assertTrue(event.isExpired());
                    }
                } else if (count == 2) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals("item4", event.getData(2));
                        Assert.assertEquals(2L, event.getData(3));
                        Assert.assertNull(event.getData(4));
                        Assert.assertNull(event.getData(5));
                        Assert.assertNull(event.getData(6));
                        Assert.assertNull(event.getData(7));
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNull(removeEvents);
                } else if (count == 2) {
                    Assert.assertNull(inEvents);
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals("item4", event.getData(2));
                        Assert.assertEquals(2L, event.getData(3));
                        Assert.assertNull(event.getData(4));
                        Assert.assertNull(event.getData(5));
                        Assert.assertNull(event.getData(6));
                        Assert.assertNull(event.getData(7));
                        Assert.assertTrue(event.isExpired());
                    }
                }
                count++;
            }
        });
    }
}
