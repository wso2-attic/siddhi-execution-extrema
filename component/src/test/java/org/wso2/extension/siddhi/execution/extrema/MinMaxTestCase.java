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
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for MinMax extension.
 */
public class MinMaxTestCase {
    private static final Logger log = Logger.getLogger(MinMaxTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;
    private AtomicInteger eventCount;
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void init() {
        count = 0;
        eventArrived = false;
        eventCount = new AtomicInteger(0);
    }

    @Test
    public void test1MinMaxStreamProcessorExtension() throws InterruptedException {
        log.info("MinMax TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int ,price double);";
        String query = ("@info(name = 'query1') from inputStream#extrema:minMax(price, 4, 5, 1.0, 2.0, 'minmax')  "
                + "select id, price, extremaType, preBound, postBound " + "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    eventCount.incrementAndGet();
                    switch (count) {
                    case 1:
                        AssertJUnit.assertEquals(98, event.getData(1));
                        AssertJUnit.assertEquals("min", event.getData(2));
                        break;
                    case 2:
                        AssertJUnit.assertEquals(103, event.getData(1));
                        AssertJUnit.assertEquals("max", event.getData(2));
                        break;
                    case 3:
                        AssertJUnit.assertEquals(102, event.getData(1));
                        AssertJUnit.assertEquals("max", event.getData(2));
                        break;
                    case 4:
                        AssertJUnit.assertEquals(107, event.getData(1));
                        AssertJUnit.assertEquals("max", event.getData(2));
                        break;
                    default:
                        AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1, 101 });
        inputHandler.send(new Object[] { 2, 98.5 });
        inputHandler.send(new Object[] { 3, 98 });
        inputHandler.send(new Object[] { 4, 103 });
        inputHandler.send(new Object[] { 5, 100 });
        inputHandler.send(new Object[] { 6, 98 });
        inputHandler.send(new Object[] { 7, 98 });
        inputHandler.send(new Object[] { 8, 102 });
        inputHandler.send(new Object[] { 9, 101 });
        inputHandler.send(new Object[] { 10, 98 });
        inputHandler.send(new Object[] { 11, 98 });
        inputHandler.send(new Object[] { 12, 98 });
        inputHandler.send(new Object[] { 13, 98 });
        inputHandler.send(new Object[] { 14, 98 });
        inputHandler.send(new Object[] { 15, 98 });
        inputHandler.send(new Object[] { 16, 98 });
        inputHandler.send(new Object[] { 17, 98 });
        inputHandler.send(new Object[] { 18, 107 });
        inputHandler.send(new Object[] { 19, 98 });
        inputHandler.send(new Object[] { 20, 98 });
        inputHandler.send(new Object[] { 21, 98 });
        inputHandler.send(new Object[] { 22, 98 });
        inputHandler.send(new Object[] { 23, 98 });
        inputHandler.send(new Object[] { 24, 98 });
        inputHandler.send(new Object[] { 25, 106 });
        inputHandler.send(new Object[] { 26, 98 });
        inputHandler.send(new Object[] { 27, 105 });

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        AssertJUnit.assertEquals(4, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test2MinMaxStreamProcessorExtension() throws InterruptedException {
        log.info("Max TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int ,price double);";
        String query = ("@info(name = 'query1') from inputStream#extrema:minMax(price, 4, 5, 1.0, 2.0, 'max')  "
                + "select id, price, extremaType, preBound, postBound " + "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    eventCount.incrementAndGet();
                    switch (count) {
                    case 1:
                        AssertJUnit.assertEquals(103, event.getData(1));
                        AssertJUnit.assertEquals("max", event.getData(2));
                        break;
                    case 2:
                        AssertJUnit.assertEquals(102, event.getData(1));
                        AssertJUnit.assertEquals("max", event.getData(2));
                        break;
                    case 3:
                        AssertJUnit.assertEquals(107, event.getData(1));
                        AssertJUnit.assertEquals("max", event.getData(2));
                        break;
                    default:
                        AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1, 101 });
        inputHandler.send(new Object[] { 2, 98.5 });
        inputHandler.send(new Object[] { 3, 98 });
        inputHandler.send(new Object[] { 4, 103 });
        inputHandler.send(new Object[] { 5, 100 });
        inputHandler.send(new Object[] { 6, 98 });
        inputHandler.send(new Object[] { 7, 98 });
        inputHandler.send(new Object[] { 8, 102 });
        inputHandler.send(new Object[] { 9, 101 });
        inputHandler.send(new Object[] { 10, 98 });
        inputHandler.send(new Object[] { 11, 98 });
        inputHandler.send(new Object[] { 12, 98 });
        inputHandler.send(new Object[] { 13, 98 });
        inputHandler.send(new Object[] { 14, 98 });
        inputHandler.send(new Object[] { 15, 98 });
        inputHandler.send(new Object[] { 16, 98 });
        inputHandler.send(new Object[] { 17, 98 });
        inputHandler.send(new Object[] { 18, 107 });
        inputHandler.send(new Object[] { 19, 98 });
        inputHandler.send(new Object[] { 20, 98 });
        inputHandler.send(new Object[] { 21, 98 });
        inputHandler.send(new Object[] { 22, 98 });
        inputHandler.send(new Object[] { 23, 98 });
        inputHandler.send(new Object[] { 24, 98 });
        inputHandler.send(new Object[] { 25, 106 });
        inputHandler.send(new Object[] { 26, 98 });
        inputHandler.send(new Object[] { 27, 105 });

        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        AssertJUnit.assertEquals(3, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void test3MinMaxStreamProcessorExtension() throws InterruptedException {
        log.info("Min TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int ,price double);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#extrema:minMax(price, 4, 5, 1.0, 2.0, 'min')  "
                + "select id, price, extremaType, preBound, postBound " + "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    eventCount.incrementAndGet();
                    switch (count) {
                    case 1:
                        AssertJUnit.assertEquals(98, event.getData(1));
                        AssertJUnit.assertEquals("min", event.getData(2));
                        break;
                    default:
                        AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1, 101 });
        inputHandler.send(new Object[] { 2, 98.5 });
        inputHandler.send(new Object[] { 3, 98 });
        inputHandler.send(new Object[] { 4, 103 });
        inputHandler.send(new Object[] { 5, 100 });
        inputHandler.send(new Object[] { 6, 98 });
        inputHandler.send(new Object[] { 7, 98 });
        inputHandler.send(new Object[] { 8, 102 });
        inputHandler.send(new Object[] { 9, 101 });
        inputHandler.send(new Object[] { 10, 98 });
        inputHandler.send(new Object[] { 11, 98 });
        inputHandler.send(new Object[] { 12, 98 });
        inputHandler.send(new Object[] { 13, 98 });
        inputHandler.send(new Object[] { 14, 98 });
        inputHandler.send(new Object[] { 15, 98 });
        inputHandler.send(new Object[] { 16, 98 });
        inputHandler.send(new Object[] { 17, 98 });
        inputHandler.send(new Object[] { 18, 107 });
        inputHandler.send(new Object[] { 19, 98 });
        inputHandler.send(new Object[] { 20, 98 });
        inputHandler.send(new Object[] { 21, 98 });
        inputHandler.send(new Object[] { 22, 98 });
        inputHandler.send(new Object[] { 23, 98 });
        inputHandler.send(new Object[] { 24, 98 });
        inputHandler.send(new Object[] { 25, 106 });
        inputHandler.send(new Object[] { 26, 98 });
        inputHandler.send(new Object[] { 27, 105 });

        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);
        AssertJUnit.assertEquals(1, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMinWhenPostBound0() throws InterruptedException {
        log.info("Min TestCase: PostBound = 0");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int ,price double);";
        String query = ("@info(name = 'query1') "
                + "from inputStream#extrema:minMax(price, 4, 0, 1.0, 0, 'min')  "
                + "select id, price, extremaType, preBound, postBound "
                + "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    eventCount.incrementAndGet();
                    switch (count) {
                    case 1:
                        AssertJUnit.assertEquals(98.5, event.getData(1));
                        AssertJUnit.assertEquals("min", event.getData(2));
                        break;
                    case 2:
                        AssertJUnit.assertEquals(98, event.getData(1));
                        AssertJUnit.assertEquals("min", event.getData(2));
                        break;
                    default:
                        AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1, 101 });
        inputHandler.send(new Object[] { 2, 98.5 });
        inputHandler.send(new Object[] { 3, 98 });
        inputHandler.send(new Object[] { 4, 103 });
        inputHandler.send(new Object[] { 5, 100 });
        inputHandler.send(new Object[] { 6, 98 });
        inputHandler.send(new Object[] { 7, 98 });
        inputHandler.send(new Object[] { 8, 102 });
        inputHandler.send(new Object[] { 9, 101 });
        inputHandler.send(new Object[] { 10, 98 });
        inputHandler.send(new Object[] { 11, 98 });
        inputHandler.send(new Object[] { 12, 98 });
        inputHandler.send(new Object[] { 13, 98 });
        inputHandler.send(new Object[] { 14, 98 });
        inputHandler.send(new Object[] { 15, 98 });
        inputHandler.send(new Object[] { 16, 98 });
        inputHandler.send(new Object[] { 17, 98 });
        inputHandler.send(new Object[] { 18, 107 });
        inputHandler.send(new Object[] { 19, 98 });
        inputHandler.send(new Object[] { 20, 98 });
        inputHandler.send(new Object[] { 21, 98 });
        inputHandler.send(new Object[] { 22, 98 });
        inputHandler.send(new Object[] { 23, 98 });
        inputHandler.send(new Object[] { 24, 98 });
        inputHandler.send(new Object[] { 25, 106 });
        inputHandler.send(new Object[] { 26, 98 });
        inputHandler.send(new Object[] { 27, 105 });

        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        AssertJUnit.assertEquals(2, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMaxWhenPostBound0() throws InterruptedException {
        log.info("Max TestCase: PostBound = 0");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int ,price double);";
        String query = ("@info(name = 'query1') "
                + "from inputStream#extrema:minMax(price, 4, 0, 1.0, 0, 'max')  "
                + "select id, price, extremaType, preBound, postBound "
                + "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    eventCount.incrementAndGet();
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(103, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(102, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(107, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(106, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1, 101 });
        inputHandler.send(new Object[] { 2, 98.5 });
        inputHandler.send(new Object[] { 3, 98 });
        inputHandler.send(new Object[] { 4, 103 });
        inputHandler.send(new Object[] { 5, 100 });
        inputHandler.send(new Object[] { 6, 98 });
        inputHandler.send(new Object[] { 7, 98 });
        inputHandler.send(new Object[] { 8, 102 });
        inputHandler.send(new Object[] { 9, 101 });
        inputHandler.send(new Object[] { 10, 98 });
        inputHandler.send(new Object[] { 11, 98 });
        inputHandler.send(new Object[] { 12, 98 });
        inputHandler.send(new Object[] { 13, 98 });
        inputHandler.send(new Object[] { 14, 98 });
        inputHandler.send(new Object[] { 15, 98 });
        inputHandler.send(new Object[] { 16, 98 });
        inputHandler.send(new Object[] { 17, 98 });
        inputHandler.send(new Object[] { 18, 107 });
        inputHandler.send(new Object[] { 19, 98 });
        inputHandler.send(new Object[] { 20, 98 });
        inputHandler.send(new Object[] { 21, 98 });
        inputHandler.send(new Object[] { 22, 98 });
        inputHandler.send(new Object[] { 23, 98 });
        inputHandler.send(new Object[] { 24, 98 });
        inputHandler.send(new Object[] { 25, 106 });
        inputHandler.send(new Object[] { 26, 98 });
        inputHandler.send(new Object[] { 27, 105 });

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        AssertJUnit.assertEquals(4, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMinWhenPreBound0() throws InterruptedException {
        log.info("Min TestCase: PreBound = 0");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int ,price double);";
        String query = ("@info(name = 'query1') "
                + "from inputStream#extrema:minMax(price, 0, 5, 0, 2.0, 'min')  "
                + "select id, price, extremaType, preBound, postBound "
                + "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    eventCount.incrementAndGet();
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(98, event.getData(1));
                            AssertJUnit.assertEquals("min", event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(98, event.getData(1));
                            AssertJUnit.assertEquals("min", event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(98, event.getData(1));
                            AssertJUnit.assertEquals("min", event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(98, event.getData(1));
                            AssertJUnit.assertEquals("min", event.getData(2));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(98, event.getData(1));
                            AssertJUnit.assertEquals("min", event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1, 101 });
        inputHandler.send(new Object[] { 2, 98.5 });
        inputHandler.send(new Object[] { 3, 98 });
        inputHandler.send(new Object[] { 4, 103 });
        inputHandler.send(new Object[] { 5, 100 });
        inputHandler.send(new Object[] { 6, 98 });
        inputHandler.send(new Object[] { 7, 98 });
        inputHandler.send(new Object[] { 8, 102 });
        inputHandler.send(new Object[] { 9, 101 });
        inputHandler.send(new Object[] { 10, 98 });
        inputHandler.send(new Object[] { 11, 98 });
        inputHandler.send(new Object[] { 12, 98 });
        inputHandler.send(new Object[] { 13, 98 });
        inputHandler.send(new Object[] { 14, 98 });
        inputHandler.send(new Object[] { 15, 98 });
        inputHandler.send(new Object[] { 16, 98 });
        inputHandler.send(new Object[] { 17, 98 });
        inputHandler.send(new Object[] { 18, 107 });
        inputHandler.send(new Object[] { 19, 98 });
        inputHandler.send(new Object[] { 20, 98 });
        inputHandler.send(new Object[] { 21, 98 });
        inputHandler.send(new Object[] { 22, 98 });
        inputHandler.send(new Object[] { 23, 98 });
        inputHandler.send(new Object[] { 24, 98 });
        inputHandler.send(new Object[] { 25, 106 });
        inputHandler.send(new Object[] { 26, 98 });
        inputHandler.send(new Object[] { 27, 105 });

        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        AssertJUnit.assertEquals(5, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMaxWhenPreBound0() throws InterruptedException {
        log.info("Max TestCase: PreBound = 0");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int ,price double);";
        String query = ("@info(name = 'query1') "
                + "from inputStream#extrema:minMax(price, 0, 5, 0, 2.0, 'max')  "
                + "select id, price, extremaType, preBound, postBound "
                + "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    eventCount.incrementAndGet();
                    switch (count) {
                        case 1:
                            AssertJUnit.assertEquals(101, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(103, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(102, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(101, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(107, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(106, event.getData(1));
                            AssertJUnit.assertEquals("max", event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1, 101 });
        inputHandler.send(new Object[] { 2, 98.5 });
        inputHandler.send(new Object[] { 3, 98 });
        inputHandler.send(new Object[] { 4, 103 });
        inputHandler.send(new Object[] { 5, 100 });
        inputHandler.send(new Object[] { 6, 98 });
        inputHandler.send(new Object[] { 7, 98 });
        inputHandler.send(new Object[] { 8, 102 });
        inputHandler.send(new Object[] { 9, 101 });
        inputHandler.send(new Object[] { 10, 98 });
        inputHandler.send(new Object[] { 11, 98 });
        inputHandler.send(new Object[] { 12, 98 });
        inputHandler.send(new Object[] { 13, 98 });
        inputHandler.send(new Object[] { 14, 98 });
        inputHandler.send(new Object[] { 15, 98 });
        inputHandler.send(new Object[] { 16, 98 });
        inputHandler.send(new Object[] { 17, 98 });
        inputHandler.send(new Object[] { 18, 107 });
        inputHandler.send(new Object[] { 19, 98 });
        inputHandler.send(new Object[] { 20, 98 });
        inputHandler.send(new Object[] { 21, 98 });
        inputHandler.send(new Object[] { 22, 98 });
        inputHandler.send(new Object[] { 23, 98 });
        inputHandler.send(new Object[] { 24, 98 });
        inputHandler.send(new Object[] { 25, 106 });
        inputHandler.send(new Object[] { 26, 98 });
        inputHandler.send(new Object[] { 27, 105 });

        SiddhiTestHelper.waitForEvents(waitTime, 6, eventCount, timeout);
        AssertJUnit.assertEquals(6, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMinMaxWithKalman() throws InterruptedException {
        log.info("MinMax on Kalman smoothened function TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int ,price double);";
        String query = ("@info(name = 'query1') " + "from inputStream "
                + "select id, kf:kalmanFilter(price) as kalmanEstimatedValue insert into inputStream1;"
                + "@info(name = 'query2') " +
                "from inputStream1#extrema:minMax(kalmanEstimatedValue, 4, 5, 0.3, 0.3, 'minmax')  "
                + "select id, kalmanEstimatedValue, extremaType, preBound, postBound insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        // Uncomment the following if you wish to see the Kalman smoothened events
        /*
         * siddhiAppRuntime.addCallback("query1", new QueryCallback() {
         * 
         * @Override
         * public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
         * EventPrinter.print(timeStamp, inEvents, removeEvents);
         * }
         * });
         */
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    count++;
                    eventCount.incrementAndGet();
                    switch (count) {
                    case 1:
                        AssertJUnit.assertEquals(99.16666727781494, event.getData(1));
                        AssertJUnit.assertEquals("min", event.getData(2));
                        break;
                    case 2:
                        AssertJUnit.assertEquals(99.94444456173554, event.getData(1));
                        AssertJUnit.assertEquals("max", event.getData(2));
                        break;
                    case 3:
                        AssertJUnit.assertEquals(99.02941188062991, event.getData(1));
                        AssertJUnit.assertEquals("min", event.getData(2));
                        break;
                    case 4:
                        AssertJUnit.assertEquals(99.47222230710395, event.getData(1));
                        AssertJUnit.assertEquals("max", event.getData(2));
                        break;
                    default:
                        AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[] { 1, 101.0 });
        inputHandler.send(new Object[] { 2, 98.5 });
        inputHandler.send(new Object[] { 3, 98.0 });
        inputHandler.send(new Object[] { 4, 103.0 });
        inputHandler.send(new Object[] { 5, 100.0 });
        inputHandler.send(new Object[] { 6, 98.0 });
        inputHandler.send(new Object[] { 7, 98.0 });
        inputHandler.send(new Object[] { 8, 102.0 });
        inputHandler.send(new Object[] { 9, 101.0 });
        inputHandler.send(new Object[] { 10, 98.0 });
        inputHandler.send(new Object[] { 11, 98.0 });
        inputHandler.send(new Object[] { 12, 98.0 });
        inputHandler.send(new Object[] { 13, 98.0 });
        inputHandler.send(new Object[] { 14, 98.0 });
        inputHandler.send(new Object[] { 15, 98.0 });
        inputHandler.send(new Object[] { 16, 98.0 });
        inputHandler.send(new Object[] { 17, 98.0 });
        inputHandler.send(new Object[] { 18, 107.0 });
        inputHandler.send(new Object[] { 19, 98.0 });
        inputHandler.send(new Object[] { 20, 98.0 });
        inputHandler.send(new Object[] { 21, 98.0 });
        inputHandler.send(new Object[] { 22, 98.0 });
        inputHandler.send(new Object[] { 23, 98.0 });
        inputHandler.send(new Object[] { 24, 98.0 });
        inputHandler.send(new Object[] { 25, 106.0 });
        inputHandler.send(new Object[] { 26, 98.0 });
        inputHandler.send(new Object[] { 27, 105.0 });

        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        AssertJUnit.assertEquals(4, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();
    }
}
