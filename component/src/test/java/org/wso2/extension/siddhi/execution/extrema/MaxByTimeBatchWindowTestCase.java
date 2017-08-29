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
 * Test case for MaxByTimeBatchWindow extension.
 */
public class MaxByTimeBatchWindowTestCase {
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
    public void maxbyTimeBatchWindowTest1() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream =
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxbytimebatch(price,1 sec) " +
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

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        siddhiAppRuntime.shutdown();;
        AssertJUnit.assertEquals(3, inEventCount);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
    @Test
    public void maxbyTimeBatchWindowTest2() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream =
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query2') " +
                "from cseEventStream#window.extrema:maxbytimebatch(price,1 sec) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
                    @Override
                    public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                        EventPrinter.print(timeStamp , inEvents , removeEvents);
                        eventCount.incrementAndGet();
                    }
                });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 1});
        inputHandler.send(new Object[]{"WSO2", 888f, 1});
        inputHandler.send(new Object[]{"MIT", 700f, 1});
        Thread.sleep(3300);
        inputHandler.send(new Object[]{"WSO2", 60.5f, 2});
        inputHandler.send(new Object[]{"IBM", 777f, 3});
        inputHandler.send(new Object[]{"WSO2", 234.5f, 4});
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"IBM", 90f, 5});
        inputHandler.send(new Object[]{"WSO2", 765f, 6});

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void maxbyTimeBatchWindowTest3() throws InterruptedException {

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream =
                "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "" +
                "@info(name = 'query2') " +
                "from cseEventStream#window.extrema:maxbytimebatch(price,1 sec) " +
                "select symbol, price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp , inEvents , removeEvents);
                eventCount.incrementAndGet();
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

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        siddhiAppRuntime.shutdown();
    }
}

