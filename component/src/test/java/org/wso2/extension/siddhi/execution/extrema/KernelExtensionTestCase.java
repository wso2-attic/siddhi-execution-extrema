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
 * Test case for KernelExtension extension.
 */
public class KernelExtensionTestCase {
    private static final Logger log = Logger.getLogger(KernelExtensionTestCase.class);
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
    public void testKernelMaxStreamProcessorExtension() throws InterruptedException {
        log.info("KernelMaxExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price double);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#extrema:kernelMinMax(price, 4, 17, 'max') " +
                "select *" +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

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
                            AssertJUnit.assertEquals(54.0, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(51.75, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(50.44, event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(48.26, event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(49.85, event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals(49.94, event.getData(0));
                            break;
                        case 7:
                            AssertJUnit.assertEquals(50.3, event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();

                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{51.7d});
        inputHandler.send(new Object[]{51.07d});
        inputHandler.send(new Object[]{51.05d});
        inputHandler.send(new Object[]{50.77d});
        inputHandler.send(new Object[]{51.3d});
        inputHandler.send(new Object[]{51.35d});
        inputHandler.send(new Object[]{51d});
        inputHandler.send(new Object[]{51.1d});
        inputHandler.send(new Object[]{50.9d});
        inputHandler.send(new Object[]{51.35d});
        inputHandler.send(new Object[]{51.2d});
        inputHandler.send(new Object[]{52.95d});
        inputHandler.send(new Object[]{53d});
        inputHandler.send(new Object[]{53.44d});
        inputHandler.send(new Object[]{54d});
        inputHandler.send(new Object[]{53.8d});
        inputHandler.send(new Object[]{53.59d});
        inputHandler.send(new Object[]{52.6d});
        inputHandler.send(new Object[]{51.88d});
        inputHandler.send(new Object[]{52.12d});
        inputHandler.send(new Object[]{51.5d});
        inputHandler.send(new Object[]{51.66d});
        inputHandler.send(new Object[]{52.3d});
        inputHandler.send(new Object[]{51.38d});
        inputHandler.send(new Object[]{51.2d});
        inputHandler.send(new Object[]{51d});
        inputHandler.send(new Object[]{50.58d});
        inputHandler.send(new Object[]{50.36d});
        inputHandler.send(new Object[]{50.35d});
        inputHandler.send(new Object[]{50.71d});
        inputHandler.send(new Object[]{51.05d});
        inputHandler.send(new Object[]{51.6d});
        inputHandler.send(new Object[]{50.81d});
        inputHandler.send(new Object[]{51d});
        inputHandler.send(new Object[]{51.5d});
        inputHandler.send(new Object[]{51.75d});
        inputHandler.send(new Object[]{51.1d});
        inputHandler.send(new Object[]{51.15d});
        inputHandler.send(new Object[]{50.96d});
        inputHandler.send(new Object[]{49.96d});
        inputHandler.send(new Object[]{49.67d});
        inputHandler.send(new Object[]{50d});
        inputHandler.send(new Object[]{48.7d});
        inputHandler.send(new Object[]{49.5d});
        inputHandler.send(new Object[]{49.47d});
        inputHandler.send(new Object[]{48.7d});
        inputHandler.send(new Object[]{48d});
        inputHandler.send(new Object[]{47.95d});
        inputHandler.send(new Object[]{48.15d});
        inputHandler.send(new Object[]{49d});
        inputHandler.send(new Object[]{48.45d});
        inputHandler.send(new Object[]{48.95d});
        inputHandler.send(new Object[]{49.42d});
        inputHandler.send(new Object[]{50.44d});
        inputHandler.send(new Object[]{49.58d});
        inputHandler.send(new Object[]{48.19d});
        inputHandler.send(new Object[]{47.3d});
        inputHandler.send(new Object[]{48.01d});
        inputHandler.send(new Object[]{46.3d});
        inputHandler.send(new Object[]{45.6d});
        inputHandler.send(new Object[]{46.37d});
        inputHandler.send(new Object[]{47.35d});
        inputHandler.send(new Object[]{47.6d});
        inputHandler.send(new Object[]{47.3d});
        inputHandler.send(new Object[]{47.56d});
        inputHandler.send(new Object[]{48.26d});
        inputHandler.send(new Object[]{48.22d});
        inputHandler.send(new Object[]{47.75d});
        inputHandler.send(new Object[]{47.25d});
        inputHandler.send(new Object[]{46.9d});
        inputHandler.send(new Object[]{47.8d});
        inputHandler.send(new Object[]{47.35d});
        inputHandler.send(new Object[]{48.44d});
        inputHandler.send(new Object[]{49.85d});
        inputHandler.send(new Object[]{49.5d});
        inputHandler.send(new Object[]{49.53d});
        inputHandler.send(new Object[]{48.8d});
        inputHandler.send(new Object[]{48.56d});
        inputHandler.send(new Object[]{48.05d});
        inputHandler.send(new Object[]{48.75d});
        inputHandler.send(new Object[]{49.2d});
        inputHandler.send(new Object[]{49.65d});
        inputHandler.send(new Object[]{49.94d});
        inputHandler.send(new Object[]{49.05d});
        inputHandler.send(new Object[]{48.95d});
        inputHandler.send(new Object[]{49.15d});
        inputHandler.send(new Object[]{49.61d});
        inputHandler.send(new Object[]{48.5d});
        inputHandler.send(new Object[]{49d});
        inputHandler.send(new Object[]{49.4d});
        inputHandler.send(new Object[]{50.3d});
        inputHandler.send(new Object[]{49.8d});
        inputHandler.send(new Object[]{50.15d});
        inputHandler.send(new Object[]{49.89d});
        inputHandler.send(new Object[]{49.6d});
        inputHandler.send(new Object[]{48.98d});
        inputHandler.send(new Object[]{48.45d});
        inputHandler.send(new Object[]{49d});
        inputHandler.send(new Object[]{48.35d});

        SiddhiTestHelper.waitForEvents(waitTime, 7, eventCount, timeout);
        AssertJUnit.assertEquals(7, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testKernelMinMaxStreamProcessorExtension() throws InterruptedException {
        log.info("KernelMinMaxExtension TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (id int, price double);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#extrema:kernelMinMax(price, 3 , 16, 'minmax') " +
                "select price, extremaType , id " +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

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
                            AssertJUnit.assertEquals(54.0, event.getData(0));
                            AssertJUnit.assertEquals("max", event.getData()[1]);
                            break;
                        case 2:
                            AssertJUnit.assertEquals(50.35, event.getData(0));
                            AssertJUnit.assertEquals("min", event.getData()[1]);
                            break;
                        case 3:
                            AssertJUnit.assertEquals(51.75, event.getData(0));
                            AssertJUnit.assertEquals("max", event.getData()[1]);
                            break;
                        case 4:
                            AssertJUnit.assertEquals(47.95, event.getData(0));
                            AssertJUnit.assertEquals("min", event.getData()[1]);
                            break;
                        case 5:
                            AssertJUnit.assertEquals(50.44, event.getData(0));
                            AssertJUnit.assertEquals("max", event.getData()[1]);
                            break;
                        case 6:
                            AssertJUnit.assertEquals(45.6, event.getData(0));
                            AssertJUnit.assertEquals("min", event.getData()[1]);
                            break;
                        case 7:
                            AssertJUnit.assertEquals(48.26, event.getData(0));
                            AssertJUnit.assertEquals("max", event.getData()[1]);
                            break;
                        case 8:
                            AssertJUnit.assertEquals(49.85, event.getData(0));
                            AssertJUnit.assertEquals("max", event.getData()[1]);
                            break;
                        case 9:
                            AssertJUnit.assertEquals(46.9, event.getData(0));
                            AssertJUnit.assertEquals("min", event.getData()[1]);
                            break;
                        case 10:
                            AssertJUnit.assertEquals(48.05, event.getData(0));
                            AssertJUnit.assertEquals("min", event.getData()[1]);
                            break;
                        case 11:
                            AssertJUnit.assertEquals(49.94, event.getData(0));
                            AssertJUnit.assertEquals("max", event.getData()[1]);
                            break;
                        case 12:
                            AssertJUnit.assertEquals(48.5, event.getData(0));
                            AssertJUnit.assertEquals("min", event.getData()[1]);
                            break;
                        case 13:
                            AssertJUnit.assertEquals(50.3, event.getData(0));
                            AssertJUnit.assertEquals("max", event.getData()[1]);
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{1, 51.7d});
        inputHandler.send(new Object[]{2, 51.07d});
        inputHandler.send(new Object[]{3, 51.05d});
        inputHandler.send(new Object[]{4, 50.77d});
        inputHandler.send(new Object[]{5, 51.3d});
        inputHandler.send(new Object[]{6, 51.35d});
        inputHandler.send(new Object[]{7, 51d});
        inputHandler.send(new Object[]{8, 51.1d});
        inputHandler.send(new Object[]{9, 50.9d});
        inputHandler.send(new Object[]{10, 51.35d});
        inputHandler.send(new Object[]{11, 51.2d});
        inputHandler.send(new Object[]{12, 52.95d});
        inputHandler.send(new Object[]{13, 53d});
        inputHandler.send(new Object[]{14, 53.44d});
        inputHandler.send(new Object[]{15, 54d});
        inputHandler.send(new Object[]{16, 53.8d});
        inputHandler.send(new Object[]{17, 53.59d});
        inputHandler.send(new Object[]{18, 52.6d});
        inputHandler.send(new Object[]{19, 51.88d});
        inputHandler.send(new Object[]{20, 52.12d});
        inputHandler.send(new Object[]{21, 51.5d});
        inputHandler.send(new Object[]{22, 51.66d});
        inputHandler.send(new Object[]{23, 52.3d});
        inputHandler.send(new Object[]{24, 51.38d});
        inputHandler.send(new Object[]{25, 51.2d});
        inputHandler.send(new Object[]{26, 51d});
        inputHandler.send(new Object[]{27, 50.58d});
        inputHandler.send(new Object[]{28, 50.36d});
        inputHandler.send(new Object[]{29, 50.35d});
        inputHandler.send(new Object[]{30, 50.71d});
        inputHandler.send(new Object[]{31, 51.05d});
        inputHandler.send(new Object[]{32, 51.6d});
        inputHandler.send(new Object[]{33, 50.81d});
        inputHandler.send(new Object[]{34, 51d});
        inputHandler.send(new Object[]{35, 51.5d});
        inputHandler.send(new Object[]{36, 51.75d});
        inputHandler.send(new Object[]{37, 51.1d});
        inputHandler.send(new Object[]{38, 51.15d});
        inputHandler.send(new Object[]{39, 50.96d});
        inputHandler.send(new Object[]{40, 49.96d});
        inputHandler.send(new Object[]{41, 49.67d});
        inputHandler.send(new Object[]{42, 50d});
        inputHandler.send(new Object[]{43, 48.7d});
        inputHandler.send(new Object[]{44, 49.5d});
        inputHandler.send(new Object[]{45, 49.47d});
        inputHandler.send(new Object[]{46, 48.7d});
        inputHandler.send(new Object[]{47, 48d});
        inputHandler.send(new Object[]{48, 47.95d});
        inputHandler.send(new Object[]{49, 48.15d});
        inputHandler.send(new Object[]{50, 49d});
        inputHandler.send(new Object[]{51, 48.45d});
        inputHandler.send(new Object[]{52, 48.95d});
        inputHandler.send(new Object[]{53, 49.42d});
        inputHandler.send(new Object[]{54, 50.44d});
        inputHandler.send(new Object[]{55, 49.58d});
        inputHandler.send(new Object[]{56, 48.19d});
        inputHandler.send(new Object[]{57, 47.3d});
        inputHandler.send(new Object[]{58, 48.01d});
        inputHandler.send(new Object[]{59, 46.3d});
        inputHandler.send(new Object[]{60, 45.6d});
        inputHandler.send(new Object[]{61, 46.37d});
        inputHandler.send(new Object[]{62, 47.35d});
        inputHandler.send(new Object[]{63, 47.6d});
        inputHandler.send(new Object[]{64, 47.3d});
        inputHandler.send(new Object[]{65, 47.56d});
        inputHandler.send(new Object[]{66, 48.26d});
        inputHandler.send(new Object[]{67, 48.22d});
        inputHandler.send(new Object[]{68, 47.75d});
        inputHandler.send(new Object[]{69, 47.25d});
        inputHandler.send(new Object[]{70, 46.9d});
        inputHandler.send(new Object[]{71, 47.8d});
        inputHandler.send(new Object[]{72, 47.35d});
        inputHandler.send(new Object[]{73, 48.44d});
        inputHandler.send(new Object[]{74, 49.85d});
        inputHandler.send(new Object[]{75, 49.5d});
        inputHandler.send(new Object[]{76, 49.53d});
        inputHandler.send(new Object[]{77, 48.8d});
        inputHandler.send(new Object[]{78, 48.56d});
        inputHandler.send(new Object[]{79, 48.05d});
        inputHandler.send(new Object[]{80, 48.75d});
        inputHandler.send(new Object[]{81, 49.2d});
        inputHandler.send(new Object[]{82, 49.65d});
        inputHandler.send(new Object[]{83, 49.94d});
        inputHandler.send(new Object[]{84, 49.05d});
        inputHandler.send(new Object[]{85, 48.95d});
        inputHandler.send(new Object[]{86, 49.15d});
        inputHandler.send(new Object[]{87, 49.61d});
        inputHandler.send(new Object[]{88, 48.5d});
        inputHandler.send(new Object[]{89, 49d});
        inputHandler.send(new Object[]{90, 49.4d});
        inputHandler.send(new Object[]{91, 50.3d});
        inputHandler.send(new Object[]{92, 49.8d});
        inputHandler.send(new Object[]{93, 50.15d});
        inputHandler.send(new Object[]{94, 49.89d});
        inputHandler.send(new Object[]{95, 49.6d});
        inputHandler.send(new Object[]{96, 48.98d});
        inputHandler.send(new Object[]{97, 48.45d});
        inputHandler.send(new Object[]{98, 49d});
        inputHandler.send(new Object[]{99, 48.35d});

        SiddhiTestHelper.waitForEvents(waitTime, 13, eventCount, timeout);
        AssertJUnit.assertEquals(13, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testKernelMinStreamProcessorExtension() throws InterruptedException {
        log.info("KernelMinExtension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (price double);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#extrema:kernelMinMax(price, 4, 16, 'min') " +
                "select *" +
                "insert into outputStream;");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(inStreamDefinition + query);

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
                            AssertJUnit.assertEquals(50.35, event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(47.95, event.getData(0));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(45.6, event.getData(0));
                            break;
                        case 4:
                            AssertJUnit.assertEquals(48.05, event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals(48.5, event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail();
                    }
                }
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("inputStream");
        siddhiAppRuntime.start();

        inputHandler.send(new Object[]{51.7d});
        inputHandler.send(new Object[]{51.07d});
        inputHandler.send(new Object[]{51.05d});
        inputHandler.send(new Object[]{50.77d});
        inputHandler.send(new Object[]{51.3d});
        inputHandler.send(new Object[]{51.35d});
        inputHandler.send(new Object[]{51d});
        inputHandler.send(new Object[]{51.1d});
        inputHandler.send(new Object[]{50.9d});
        inputHandler.send(new Object[]{51.35d});
        inputHandler.send(new Object[]{51.2d});
        inputHandler.send(new Object[]{52.95d});
        inputHandler.send(new Object[]{53d});
        inputHandler.send(new Object[]{53.44d});
        inputHandler.send(new Object[]{54d});
        inputHandler.send(new Object[]{53.8d});
        inputHandler.send(new Object[]{53.59d});
        inputHandler.send(new Object[]{52.6d});
        inputHandler.send(new Object[]{51.88d});
        inputHandler.send(new Object[]{52.12d});
        inputHandler.send(new Object[]{51.5d});
        inputHandler.send(new Object[]{51.66d});
        inputHandler.send(new Object[]{52.3d});
        inputHandler.send(new Object[]{51.38d});
        inputHandler.send(new Object[]{51.2d});
        inputHandler.send(new Object[]{51d});
        inputHandler.send(new Object[]{50.58d});
        inputHandler.send(new Object[]{50.36d});
        inputHandler.send(new Object[]{50.35d});
        inputHandler.send(new Object[]{50.71d});
        inputHandler.send(new Object[]{51.05d});
        inputHandler.send(new Object[]{51.6d});
        inputHandler.send(new Object[]{50.81d});
        inputHandler.send(new Object[]{51d});
        inputHandler.send(new Object[]{51.5d});
        inputHandler.send(new Object[]{51.75d});
        inputHandler.send(new Object[]{51.1d});
        inputHandler.send(new Object[]{51.15d});
        inputHandler.send(new Object[]{50.96d});
        inputHandler.send(new Object[]{49.96d});
        inputHandler.send(new Object[]{49.67d});
        inputHandler.send(new Object[]{50d});
        inputHandler.send(new Object[]{48.7d});
        inputHandler.send(new Object[]{49.5d});
        inputHandler.send(new Object[]{49.47d});
        inputHandler.send(new Object[]{48.7d});
        inputHandler.send(new Object[]{48d});
        inputHandler.send(new Object[]{47.95d});
        inputHandler.send(new Object[]{48.15d});
        inputHandler.send(new Object[]{49d});
        inputHandler.send(new Object[]{48.45d});
        inputHandler.send(new Object[]{48.95d});
        inputHandler.send(new Object[]{49.42d});
        inputHandler.send(new Object[]{50.44d});
        inputHandler.send(new Object[]{49.58d});
        inputHandler.send(new Object[]{48.19d});
        inputHandler.send(new Object[]{47.3d});
        inputHandler.send(new Object[]{48.01d});
        inputHandler.send(new Object[]{46.3d});
        inputHandler.send(new Object[]{45.6d});
        inputHandler.send(new Object[]{46.37d});
        inputHandler.send(new Object[]{47.35d});
        inputHandler.send(new Object[]{47.6d});
        inputHandler.send(new Object[]{47.3d});
        inputHandler.send(new Object[]{47.56d});
        inputHandler.send(new Object[]{48.26d});
        inputHandler.send(new Object[]{48.22d});
        inputHandler.send(new Object[]{47.75d});
        inputHandler.send(new Object[]{47.25d});
        inputHandler.send(new Object[]{46.9d});
        inputHandler.send(new Object[]{47.8d});
        inputHandler.send(new Object[]{47.35d});
        inputHandler.send(new Object[]{48.44d});
        inputHandler.send(new Object[]{49.85d});
        inputHandler.send(new Object[]{49.5d});
        inputHandler.send(new Object[]{49.53d});
        inputHandler.send(new Object[]{48.8d});
        inputHandler.send(new Object[]{48.56d});
        inputHandler.send(new Object[]{48.05d});
        inputHandler.send(new Object[]{48.75d});
        inputHandler.send(new Object[]{49.2d});
        inputHandler.send(new Object[]{49.65d});
        inputHandler.send(new Object[]{49.94d});
        inputHandler.send(new Object[]{49.05d});
        inputHandler.send(new Object[]{48.95d});
        inputHandler.send(new Object[]{49.15d});
        inputHandler.send(new Object[]{49.61d});
        inputHandler.send(new Object[]{48.5d});
        inputHandler.send(new Object[]{49d});
        inputHandler.send(new Object[]{49.4d});
        inputHandler.send(new Object[]{50.3d});
        inputHandler.send(new Object[]{49.8d});
        inputHandler.send(new Object[]{50.15d});
        inputHandler.send(new Object[]{49.89d});
        inputHandler.send(new Object[]{49.6d});
        inputHandler.send(new Object[]{48.98d});
        inputHandler.send(new Object[]{48.45d});
        inputHandler.send(new Object[]{49d});
        inputHandler.send(new Object[]{48.35d});


        SiddhiTestHelper.waitForEvents(waitTime, 5, eventCount, timeout);
        AssertJUnit.assertEquals(5, count);
        AssertJUnit.assertTrue(eventArrived);
        siddhiAppRuntime.shutdown();

    }

}
