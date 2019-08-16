
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
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for MaxByLengthWindowProcessor extension.
 */
public class MaxByLengthWindowProcessorTestCase {
    private static final Logger log = Logger.getLogger(MaxByLengthWindowProcessorTestCase.class);
    int count;
    private AtomicInteger eventCount;
    private int waitTime = 50;
    private int timeout = 30000;
    List<Object> results = new ArrayList<Object>();

    @BeforeMethod
    public void init() {
        count = 0;
        eventCount = new AtomicInteger(0);
    }


    @Test
    public void testMaxByLengthWindowProcessor1() throws InterruptedException {
        log.info("Testing maxByLengthWindowProcessor with no of events less than window size for " +
                "float type parameter");
        SiddhiManager siddhiManager = new SiddhiManager();


        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLength(price, 4) select symbol,price," +
                "volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        results.add(new Object[]{"IBM", 700f, 14});
        results.add(new Object[]{"IBM", 700f, 14});
        results.add(new Object[]{"WSO2", 790f, 1});
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
            inputHandler.send(new Object[]{"WSO2", 790f, 1});
            SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testMaxByLengthWindowProcessor10() throws InterruptedException {
        log.info("Testing maxByLengthWindowProcessor with no of events less than window size for " +
                "float type parameter");
        SiddhiManager siddhiManager = new SiddhiManager();


        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLength(price, 2) " +
                "select symbol,price," +
                "volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    log.info("output event: ");
                    EventPrinter.print(events);

                    for (Event event : events) {
                        count++;
                    }
                    eventCount.incrementAndGet();
                }
            });
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"IBM", 10f, 14});
            inputHandler.send(new Object[]{"IBM", 20.5f, 2});
            inputHandler.send(new Object[]{"WSO2", 20.5f, 1});
            inputHandler.send(new Object[]{"WSO2", 23f, 1});

            SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testMaxByLengthWindowProcessor2() throws InterruptedException {
        log.info("Testing maxByLengthWindowProcessor with no of events equal to window size for " +
                "integer type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLength(volume, 4) select symbol,price," +
                "volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"IBM", 700f, 14});
            results.add(new Object[]{"IBM", 700f, 14});
            results.add(new Object[]{"IBM", 700f, 20});
            results.add(new Object[]{"ZZZ", 60.5f, 82});
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
            inputHandler.send(new Object[]{"IBM", 60.5f, 12});
            inputHandler.send(new Object[]{"IBM", 700f, 20});
            inputHandler.send(new Object[]{"ZZZ", 60.5f, 82});
            SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testMaxByLengthWindowProcessor3() throws InterruptedException {
        log.info("Testing maxByLengthWindowProcessor with no of events greater than window size for " +
                "String type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLength(symbol, 4) select symbol,price," +
                "volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"bbc", 700f, 14});
            results.add(new Object[]{"bbc", 700f, 14});
            results.add(new Object[]{"xxx", 700f, 2});
            results.add(new Object[]{"xxx", 700f, 2});
            results.add(new Object[]{"xxx", 700f, 2});
            results.add(new Object[]{"zzz", 60.5f, 12});
            results.add(new Object[]{"zzz", 60.5f, 12});
            results.add(new Object[]{"zzz", 60.5f, 12});
            results.add(new Object[]{"zzz", 60.5f, 12});
            results.add(new Object[]{"rye", 60.5f, 82});

            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    log.info("output event: ");
                    EventPrinter.print(events);
                    for (Event event : events) {
                        AssertJUnit.assertArrayEquals((Object[]) results.get(count), event.getData());
                        count++;
                    }
                }
            });
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"bbc", 700f, 14});
            inputHandler.send(new Object[]{"bab", 60.5f, 12});
            inputHandler.send(new Object[]{"xxx", 700f, 2});
            inputHandler.send(new Object[]{"ddd", 60.5f, 82});
            inputHandler.send(new Object[]{"abc", 700f, 84});
            inputHandler.send(new Object[]{"zzz", 60.5f, 12});
            inputHandler.send(new Object[]{"aaa", 700f, 2});
            inputHandler.send(new Object[]{"dhh", 60.5f, 82});
            inputHandler.send(new Object[]{"drg", 700f, 2});
            inputHandler.send(new Object[]{"rye", 60.5f, 82});
            Thread.sleep(1000);
        } finally {
            siddhiAppRuntime.shutdown();
        }
    }


    @Test
    public void testMaxByWindowForLength4() throws InterruptedException {
        log.info("Join test1");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string,likes int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(3) " +
                "join twitterStream#window.extrema:maxByLength(likes, 2) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price, " +
                "twitterStream.likes " +
                "insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(streams + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    log.info("output event: ");
                    EventPrinter.print(events);
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();

            cseEventStreamHandler.send(new Object[]{"XXX", 699f, 14});
            cseEventStreamHandler.send(new Object[]{"ABC", 60.5f, 2});
            cseEventStreamHandler.send(new Object[]{"WSO2", 700f, 142});

            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2", 43});
            twitterStreamHandler.send(new Object[]{"User1", "Hello SIDDHI", "WSO2", 45});

            cseEventStreamHandler.send(new Object[]{"ACD", 60.5f, 21});
            cseEventStreamHandler.send(new Object[]{"XXX", 700f, 14});
            cseEventStreamHandler.send(new Object[]{"WSO2", 60.5f, 222});

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testMaxByWindowForLength6() throws InterruptedException {
        log.info("Join test2");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (num float, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLength(price, 2) " +
                "join twitterStream#window.extrema:maxByLength(num, 2) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(streams + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    EventPrinter.print(events);
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();

            cseEventStreamHandler.send(new Object[]{"WSO2", 10f, 14});
            cseEventStreamHandler.send(new Object[]{"AAA", 60.5f, 2});

            twitterStreamHandler.send(new Object[]{100, "Hello World", "XXX"});
            twitterStreamHandler.send(new Object[]{101, "Hello SIDDHI", "WSO2"});
            twitterStreamHandler.send(new Object[]{104, "Hello CEP", "WSO2"});

        } finally {
            siddhiAppRuntime.shutdown();
        }

    }


    @Test
    public void testMaxByWindowForLength7() throws InterruptedException {
        log.info("Join test3");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (num float, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLength(price, 2) " +
                "join twitterStream#window.length(2) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(streams + query);
        try {

            siddhiAppRuntime.addCallback("query1", new QueryCallback() {
                @Override
                public void receive(long l, Event[] events, Event[] events1) {
                    EventPrinter.print(l, events, events1);
                }
            });
            InputHandler cseEventStreamHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler = siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();

            cseEventStreamHandler.send(new Object[]{"WSO2", 700f, 14});
            cseEventStreamHandler.send(new Object[]{"ABC", 60.5f, 2});

            twitterStreamHandler.send(new Object[]{100, "Hello World", "XXX"});
            twitterStreamHandler.send(new Object[]{101, "Hello SIDDHI", "WSO2"});
            Thread.sleep(100);
            cseEventStreamHandler.send(new Object[]{"WSO2", 900f, 14});
            cseEventStreamHandler.send(new Object[]{"XXX", 70.5f, 2});

        } finally {
            siddhiAppRuntime.shutdown();
        }

    }


}
