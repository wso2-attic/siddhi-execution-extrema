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

package org.wso2.extension.siddhi.execution.extrema;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for MaxByTimeWindow extension.
 */
public class MaxByTimeWindowTestCase {

    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;
    private AtomicInteger eventCount;
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);
    }

    /**
     * Commenting out intermittent failing test case until fix this properly.
     */

    @Test
    public void maxbyTimeWindowTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.extrema:maxbytime(price, 1 sec) " +
                "select symbol,price, volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents", inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
                eventCount.incrementAndGet();
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"ABC", 60.4f, 2});
        inputHandler.send(new Object[]{"IBM", 60.9f, 3});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"RRR", 700f, 4});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 5});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 60.50f, 6});
        inputHandler.send(new Object[]{"AAA", 600.5f, 7});

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void maxbyTimeWindowTest2() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.extrema:maxbytime(price, 1 sec) " +
                "select symbol,price, volume " +
                "insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents",
                            inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
                eventCount.incrementAndGet();
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"MIT", 700f, 2});
        inputHandler.send(new Object[]{"WSO2", 700f, 3});

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void maxbyTimeWindowTest3() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxbytime(symbol, 1 sec) " +
                "select symbol, price " +
                "insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
                eventCount.incrementAndGet();
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"MIT", 23.5f, 3});
        inputHandler.send(new Object[]{"GOOGLE", 545.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"ORACLE", 10f, 5});
        inputHandler.send(new Object[]{"WSO2", 34.5f, 6});
        inputHandler.send(new Object[]{"GOOGLE", 65.5f, 7});
        inputHandler.send(new Object[]{"MIT", 7.5f, 8});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"GOOGLE", 7f, 9});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 10});
        inputHandler.send(new Object[]{"MIT", 632.5f, 11});

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);

    }

    @Test
    public void maxbyTimeWindowTest4() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.extrema:maxbytime(price, 1 sec) " +
                "select symbol,price," +
                "volume insert expired events into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query1",
                new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp, inEvents, removeEvents);
                        if (inEvents != null) {
                            inEventCount = inEventCount + inEvents.length;
                        }
                        if (removeEvents != null) {
                            removeEventCount = removeEventCount + removeEvents.length;
                        }
                        eventArrived = true;
                        eventCount.incrementAndGet();
                    }

                });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 798f, 2});
        inputHandler.send(new Object[]{"MIT", 432f, 3});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IFS", 700f, 4});
        inputHandler.send(new Object[]{"GOOGLE", 798f, 5});
        inputHandler.send(new Object[]{"YAHOO", 432f, 6});
        inputHandler.send(new Object[]{"GOOGLE", 798f, 7});
        inputHandler.send(new Object[]{"YAHOO", 432f, 8});

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void maxbyTimeWindowTest6() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.extrema:maxbytime(price, 1 sec) " +
                "select symbol,price, volume " +
                "insert all events into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                }
                if (removeEvents != null) {
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents",
                            inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
                eventCount.incrementAndGet();
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 98f, 2});
        inputHandler.send(new Object[]{"MIT", 432f, 3});
        Thread.sleep(1100);
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IFS", 700f, 4});
        inputHandler.send(new Object[]{"GOOGLE", 798f, 5});
        inputHandler.send(new Object[]{"YAHOO", 432f, 6});
        inputHandler.send(new Object[]{"GOOGLE", 798f, 7});
        inputHandler.send(new Object[]{"YAHOO", 32f, 8});

        SiddhiTestHelper.waitForEvents(waitTime, 6, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(4, inEventCount);
        AssertJUnit.assertEquals(4, removeEventCount);
    }


}


