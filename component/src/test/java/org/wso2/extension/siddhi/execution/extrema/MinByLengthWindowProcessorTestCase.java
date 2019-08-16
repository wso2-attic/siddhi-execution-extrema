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
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for MinByLengthWindowProcessor extension.
 */
public class MinByLengthWindowProcessorTestCase {

    private static final Logger log = Logger.getLogger(MinByLengthWindowProcessorTestCase.class);
    int count;
    List<Object> results = new ArrayList<Object>();
    private AtomicInteger eventCount;
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void init() {
        count = 0;
        eventCount = new AtomicInteger(0);
    }


    @Test
    public void testMinByLengthWindowProcessor1() throws InterruptedException {
        log.info("Testing minByLengthWindowProcessor with no of events less than window size for " +
                "float type parameter");
        SiddhiManager siddhiManager = new SiddhiManager();


        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.extrema:minByLength(price, 4) " +
                "select symbol,price," +
                "volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        results.add(new Object[]{"IBM", 700f, 14});
        results.add(new Object[]{"IBM", 20.5f, 2});
        results.add(new Object[]{"IBM", 20.5f, 2});
        try {
            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    log.info("output event: ");
                    EventPrinter.print(events);

                    for (Event event : events) {
                        AssertJUnit.assertArrayEquals((Object[]) results.get(count), event.getData());
                        count++;
                    }
                    eventCount.incrementAndGet();
                }
            });
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 20.5f, 2});
            inputHandler.send(new Object[]{"WSO2", 700f, 1});
            SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testMinByLengthWindowProcessor2() throws InterruptedException {
        log.info("Testing minByLengthWindowProcessor with no of events equal to window size for " +
                "integer type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.extrema:minByLength(volume, 4) " +
                "select symbol,price," +
                "volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"IBM", 700f, 14});
            results.add(new Object[]{"IBM", 60.5f, 12});
            results.add(new Object[]{"IBM", 700f, 2});
            results.add(new Object[]{"IBM", 700f, 2});
            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    log.info("output event: ");
                    eventCount.incrementAndGet();
                    EventPrinter.print(events);
                    for (Event event : events) {
                        count++;
                    }
                }
            });
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 60.5f, 12});
            inputHandler.send(new Object[]{"IBM", 700f, 2});
            inputHandler.send(new Object[]{"xoo", 60.5f, 82});
            SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
            } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testMinByLengthWindowProcessor3() throws InterruptedException {
        log.info("Testing minByLengthWindowProcessor with no of events greater than window size " +
                "for String type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.extrema:minByLength(symbol, 4) " +
                "select symbol,price," +
                "volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"bbc", 700f, 14});
            results.add(new Object[]{"bbb", 60.5f, 12});
            results.add(new Object[]{"bbb", 60.5f, 12});
            results.add(new Object[]{"bbb", 60.5f, 12});
            results.add(new Object[]{"abc", 700f, 84});
            results.add(new Object[]{"abc", 700f, 84});
            results.add(new Object[]{"aac", 700f, 2});
            results.add(new Object[]{"aac", 700f, 2});
            results.add(new Object[]{"aac", 700f, 2});
            results.add(new Object[]{"aaa", 60.5f, 82});

            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    log.info("output event: ");
                    EventPrinter.print(events);
                    eventCount.incrementAndGet();
                    for (Event event : events) {
                        AssertJUnit.assertArrayEquals((Object[]) results.get(count), event.getData());
                        count++;
                    }
                }
            });
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"bbc", 700f, 14});
            inputHandler.send(new Object[]{"bbb", 60.5f, 12});
            inputHandler.send(new Object[]{"xxx", 700f, 2});
            inputHandler.send(new Object[]{"ddd", 60.5f, 82});
            inputHandler.send(new Object[]{"abc", 700f, 84});
            inputHandler.send(new Object[]{"ghj", 60.5f, 12});
            inputHandler.send(new Object[]{"aac", 700f, 2});
            inputHandler.send(new Object[]{"dhh", 60.5f, 82});
            inputHandler.send(new Object[]{"drg", 700f, 2});
            inputHandler.send(new Object[]{"aaa", 60.5f, 82});
            SiddhiTestHelper.waitForEvents(waitTime, 10, eventCount, timeout);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

}
