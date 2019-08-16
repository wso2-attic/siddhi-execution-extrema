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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for MinByTimeWindow extension.
 */
public class MinByTimeWindowTestCase {

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
    public void minbyTimeWindowTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:minbytime(price, 1 sec) select symbol,price," +
                "volume insert into outputStream ;";
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
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
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

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void minbyTimeWindowTest2() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:minbytime(price, 1 sec) select symbol,price," +
                "volume insert into outputStream ;";
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
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"IBM", 798f, 1});
        inputHandler.send(new Object[]{"IBM", 432f, 1});

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        AssertJUnit.assertEquals(2, inEventCount);
        AssertJUnit.assertEquals(0, removeEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void minbyTimeWindowTest3() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:minbytime(price, 1 sec) " +
                "select symbol, price " +
                "insert expired events into outputStream ;";
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
                    eventCount.addAndGet(removeEvents.length);
                }
                eventArrived = true;
            }

        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"GOOGLE", 45.5f, 4});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"MIT", 23.5f, 3});
        Thread.sleep(2100);

        inputHandler.send(new Object[]{"ORACLE", 10f, 5});
        inputHandler.send(new Object[]{"WSO2", 34.5f, 6});
        inputHandler.send(new Object[]{"GOOGLE", 65.5f, 7});
        inputHandler.send(new Object[]{"MIT", 7.5f, 8});
        Thread.sleep(2100);
        inputHandler.send(new Object[]{"GOOGLE", 7f, 9});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 10});
        inputHandler.send(new Object[]{"MIT", 632.5f, 11});

        SiddhiTestHelper.waitForEvents(waitTime, 7, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(0, inEventCount);
        AssertJUnit.assertEquals(7, removeEventCount);

    }

    @Test
    public void minbyTimeWindowTest4() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:minbytime(price, 1 sec) select symbol,price," +
                "volume insert expired events into outputStream ;";
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
                    eventCount.addAndGet(removeEvents.length);
                }
                eventArrived = true;
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

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(0, inEventCount);
        AssertJUnit.assertEquals(5, removeEventCount);
    }

    @Test
    public void minbyTimeWindowTest6() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:minbytime(price, 1 sec) select symbol,price," +
                "volume insert all events into outputStream ;";
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
                    eventCount.addAndGet(removeEvents.length);
                }
                eventArrived = true;
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

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        siddhiAppRuntime.shutdown();
        AssertJUnit.assertEquals(5, inEventCount);
        AssertJUnit.assertEquals(5, removeEventCount);
    }

    @Test
    public void minbyTimeWindowTest5() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from twitterStream#window.extrema:minbytime(company,1 sec) " +
                "join cseEventStream#window.length(2) " +
                "on cseEventStream.symbol == twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(streams + query);
        try {

            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    eventCount.incrementAndGet();
                    EventPrinter.print(events);
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();
            cseEventStreamHandler.send(new Object[]{"IBM", 75.6f, 100});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            twitterStreamHandler.send(new Object[]{"User2", "Hi", "IBM"});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});


//            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
//            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
//            Thread.sleep(1000);
            //AssertJUnit.assertTrue("In Events can be 1 or 2 ", inEventCount == 1 || inEventCount == 2);
            SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
            AssertJUnit.assertEquals(0, removeEventCount);
//            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void minbyTimeWindowTest7() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from twitterStream#window.extrema:maxbytime(company,1 sec) " +
                "join cseEventStream#window.extrema:minbytime(price,1 sec) " +
                "on cseEventStream.symbol == twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(streams + query);
        try {

            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    EventPrinter.print(events);
                    eventCount.incrementAndGet();
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();
            cseEventStreamHandler.send(new Object[]{"IBM", 25.0f, 100});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});
            twitterStreamHandler.send(new Object[]{"User2", "Hi", "IBM"});
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            Thread.sleep(1500);
            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2"});
            cseEventStreamHandler.send(new Object[]{"WSO2", 57.6f, 100});

            SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
            //AssertJUnit.assertTrue("In Events can be 1 or 2 ", inEventCount == 1 || inEventCount == 2);
            AssertJUnit.assertEquals(0, removeEventCount);
//            AssertJUnit.assertTrue(eventArrived);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

}



