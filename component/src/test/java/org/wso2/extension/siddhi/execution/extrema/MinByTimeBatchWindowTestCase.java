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
 * Test case for MinByLengthWindowProcessor extension.
 */
public class MinByTimeBatchWindowTestCase {
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
    public void minbyTimeBatchWindowTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream =
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:minbytimebatch(price,1 sec) " +
                "select symbol, price " +
                "insert  into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    inEventCount = inEventCount + inEvents.length;
                    eventCount.addAndGet(inEvents.length);
                }
                if (removeEvents != null) {
                    AssertJUnit.assertTrue("InEvents arrived before RemoveEvents",
                            inEventCount > removeEventCount);
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 888f, 1});
        inputHandler.send(new Object[]{"MIT", 700f, 1});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 777f, 3});
        inputHandler.send(new Object[]{"WSO2", 234.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 90f, 5});
        inputHandler.send(new Object[]{"WSO2", 765f, 6});

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
    @Test
    public void minbyTimeBatchWindowTest2() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream =
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query2') " +
                "from cseEventStream#window.extrema:minbytimebatch(price,1 sec) " +
                "select symbol, price " +
                "insert expired events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
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
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 888f, 1});
        inputHandler.send(new Object[]{"MIT", 700f, 1});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 777f, 3});
        inputHandler.send(new Object[]{"WSO2", 234.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 90f, 5});
        inputHandler.send(new Object[]{"WSO2", 765f, 6});
        Thread.sleep(2000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void minbyTimeBatchWindowTest3() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream =
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query2') " +
                "from cseEventStream#window.extrema:minbytimebatch(price,1 sec) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp , inEvents , removeEvents);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 888f, 1});
        inputHandler.send(new Object[]{"MIT", 700f, 1});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 777f, 3});
        inputHandler.send(new Object[]{"WSO2", 234.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 90f, 5});
        inputHandler.send(new Object[]{"WSO2", 765f, 6});
        Thread.sleep(2000);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void minbyTimeBatchWindowTest4() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:minbytimebatch(price,1 sec) " +
                "join twitterStream#window.timeBatch(1 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    eventCount.incrementAndGet();
                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.
                    getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.
                    getInputHandler("twitterStream");
            siddhiAppRuntime.start();
            cseEventStreamHandler.send(new Object[]{"WSO2", 55.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
//            Thread.sleep(1500);
//            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            //AssertJUnit.assertTrue("In Events can be 1 or 2 ", inEventCount == 1 || inEventCount == 2);
            AssertJUnit.assertEquals(0, removeEventCount);
//            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void minbyTimeBatchWindowTest5() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:minbytimebatch(price,1 sec) " +
                "join twitterStream#window.extrema:maxbytimebatch(company,1 sec)  " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    if (inEvents != null) {
                        inEventCount += (inEvents.length);
                    }
                    if (removeEvents != null) {
                        removeEventCount += (removeEvents.length);
                        AssertJUnit.assertTrue("InEvents arrived before RemoveEvents",
                                inEventCount > removeEventCount);
                    }
                    eventArrived = true;
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();
            twitterStreamHandler.send(new Object[]{"User2", "Hi", "MIT"});
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"MIT", 57.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hey", "MIT"});
            twitterStreamHandler.send(new Object[]{"User1", "Hey", "APPLE"});
            cseEventStreamHandler.send(new Object[]{"ZET", 6.6f, 100});
            cseEventStreamHandler.send(new Object[]{"MIT", 57.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hey", "ZET"});
            Thread.sleep(1500);
            twitterStreamHandler.send(new Object[]{"User1", "Hello ", "IBM"});
            cseEventStreamHandler.send(new Object[]{"WSO2", 17.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello ", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"WSO2", 75.6f, 100});
            twitterStreamHandler.send(new Object[]{"User1", "Hello ", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"WSO2", 7.6f, 100});
            Thread.sleep(1000);
            //AssertJUnit.assertTrue("In Events can be 1 or 2 ", inEventCount == 1 || inEventCount == 2);
            AssertJUnit.assertEquals(0, removeEventCount);
//            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }
}

