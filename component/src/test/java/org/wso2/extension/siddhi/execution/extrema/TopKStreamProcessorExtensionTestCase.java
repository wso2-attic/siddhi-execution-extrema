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

import java.util.Arrays;

public class TopKStreamProcessorExtensionTestCase {
    private static final Logger log = Logger.getLogger(TopKStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testTopKStreamProcessorExtensionWithLengthBatchAndCurrentEventsOnly() throws InterruptedException {
        log.info("TopKStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#window.lengthBatch(6)#extrema:topK(item, 3)  " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (count == 0) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals(
                                Arrays.asList("item3", 64L, "item1", 3L, "item2", 2L, "item3", 1L),
                                Arrays.asList(event.getData())
                        );
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNull(removeEvents);
                } else if (count == 1) {
                    Assert.assertNull(inEvents);
                    Assert.assertNull(removeEvents);
                } else if (count == 2) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals(
                                Arrays.asList("item6", 87L, "item6", 2L, "item5", 2L, "item4", 2L),
                                Arrays.asList(event.getData())
                        );
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNull(removeEvents);
                }
                count++;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

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

        Thread.sleep(1000);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testTopKStreamProcessorExtensionWithTimeWindow() throws InterruptedException {
        log.info("BottomKStreamProcessor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#window.time(1 sec)#extrema:topK(item, 3) " +
                "insert all events into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (count == 2) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals(
                                Arrays.asList("item1", 10L, "item1", 2L, "item2", 1L, null, null),
                                Arrays.asList(event.getData())
                        );
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals(
                                Arrays.asList("item1", 10L, "item1", 1L, "item2", 1L, null, null),
                                Arrays.asList(event.getData())
                        );
                        Assert.assertTrue(event.isExpired());
                    }
                } else if (count == 8) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals(
                                Arrays.asList("item6", 58L, "item6", 1L, "item5", 1L, "item4", 1L),
                                Arrays.asList(event.getData())
                        );
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals(
                                Arrays.asList("item5", 45L, "item5", 1L, "item4", 1L, null, null),
                                Arrays.asList(event.getData())
                        );
                        Assert.assertTrue(event.isExpired());
                    }
                }
                count++;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

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

        Thread.sleep(1100);
        Assert.assertEquals(12, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}
