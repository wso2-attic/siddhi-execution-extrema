
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

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test case for MaxByLengthBatchWindowProcessor extension.
 */
public class MaxByLengthBatchWindowProcessorTestCase {
    private static final Logger log = Logger.getLogger(MaxByLengthBatchWindowProcessorTestCase.class);
    private int count;
    private AtomicInteger eventCount;
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeMethod
    public void init() {
        count = 0;
        eventCount = new AtomicInteger(0);
    }


    @Test
    public void testMaxByWindowForLengthBatch() throws InterruptedException {
        log.info("Testing maxByLengthBatchWindowProcessor with no of events equal to " +
                "window size for float parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLengthBatch(price, 4) " +
                "select symbol,price, volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        try {
            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    log.info("output event: ");
                    EventPrinter.print(events);
                    Object[] results = new Object[]{"dg", 900f, 24};
                    AssertJUnit.assertArrayEquals(results, events[0].getData());

                }
            });
            InputHandler inputHandler = siddhiAppRuntime.getInputHandler("cseEventStream");
            siddhiAppRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 40.5f, 2});
            inputHandler.send(new Object[]{"et", 900f, 1});
            inputHandler.send(new Object[]{"dg", 900f, 24});

            Thread.sleep(1000);

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testMaxByWindowForLengthBatch2() throws InterruptedException {
        log.info("Testing maxByLengthBatchWindowProcessor with no of events greater than window size " +
                "for int parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLengthBatch(volume, 4) " +
                "select symbol,price, volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"IBM", 700f, 142});
            results.add(new Object[]{"dg", 60.5f, 24});

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
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 60.5f, 2});
            inputHandler.send(new Object[]{"IBM", 700f, 142});
            inputHandler.send(new Object[]{"IBM", 60.5f, 21});
            inputHandler.send(new Object[]{"et", 700f, 1});
            inputHandler.send(new Object[]{"dg", 60.5f, 24});
            inputHandler.send(new Object[]{"IBM", 60.5f, 21});
            inputHandler.send(new Object[]{"et", 700f, 1});
            inputHandler.send(new Object[]{"dg", 60.5f, 24});
            Thread.sleep(1000);

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testMaxByWindowForLengthBatch3() throws InterruptedException {
        log.info("Testing maxByLengthBatchWindowProcessor with no of events greater than window size " +
                "for String parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.extrema:maxByLengthBatch(symbol, 4) " +
                "select symbol,price, volume insert into outputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"XXX", 700f, 14});
            results.add(new Object[]{"YTX", 60.5f, 24});

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
            inputHandler.send(new Object[]{"XXX", 700f, 14});
            inputHandler.send(new Object[]{"ABC", 60.5f, 2});
            inputHandler.send(new Object[]{"AAA", 700f, 142});
            inputHandler.send(new Object[]{"ACD", 60.5f, 21});
            inputHandler.send(new Object[]{"RTE", 700f, 1});
            inputHandler.send(new Object[]{"YTX", 60.5f, 24});
            inputHandler.send(new Object[]{"DGF", 60.5f, 21});
            inputHandler.send(new Object[]{"ETR", 700f, 1});
            inputHandler.send(new Object[]{"DXD", 60.5f, 24});

            Thread.sleep(1000);


        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testMaxByWindowForLengthBatch4() throws InterruptedException {
        log.info("Join test1");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (user string, tweet string, company string,likes int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.lengthBatch(3) " +
                "join twitterStream#window.extrema:maxByLengthBatch(likes, 2) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, " +
                "cseEventStream.price, twitterStream.likes " +
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
            InputHandler cseEventStreamHandler =
                    siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler =
                    siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();

            cseEventStreamHandler.send(new Object[]{"XXX", 700f, 14});
            cseEventStreamHandler.send(new Object[]{"ABC", 60.5f, 2});
            cseEventStreamHandler.send(new Object[]{"WSO2", 700f, 142});

            twitterStreamHandler.send(new Object[]{"User1", "Hello World", "WSO2", 23});
            twitterStreamHandler.send(new Object[]{"User1", "Hello SIDDHI", "WSO2", 56});

            cseEventStreamHandler.send(new Object[]{"ACD", 60.5f, 21});
            cseEventStreamHandler.send(new Object[]{"XXX", 700f, 14});
            cseEventStreamHandler.send(new Object[]{"WSO2", 60.5f, 222});

        } finally {
            siddhiAppRuntime.shutdown();
        }
    }

    @Test
    public void testMaxByWindowForLengthBatch6() throws InterruptedException {
        log.info("Join test2");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (num float, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLengthBatch(price, 2) " +
                "join twitterStream#window.extrema:maxByLengthBatch(num, 2) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert events into outputStream ;";

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

            cseEventStreamHandler.send(new Object[]{"WSO2", 700f, 14});
            cseEventStreamHandler.send(new Object[]{"ABC", 60.5f, 2});

            twitterStreamHandler.send(new Object[]{100, "Hello World", "XXX"});
            twitterStreamHandler.send(new Object[]{101, "Hello SIDDHI", "WSO2"});

            cseEventStreamHandler.send(new Object[]{"ACD", 60.5f, 21});
            cseEventStreamHandler.send(new Object[]{"XXX", 700f, 14});
            cseEventStreamHandler.send(new Object[]{"WSO2", 60.5f, 222});

        } finally {
            siddhiAppRuntime.shutdown();
        }

    }


    @Test
    public void testMaxByWindowForLengthBatch7() throws InterruptedException {
        log.info("Join test3");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int); " +
                "define stream twitterStream (num float, tweet string, company string); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.extrema:maxByLengthBatch(price, 2) " +
                "join twitterStream#window.lengthBatch(2) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert events into outputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.
                createSiddhiAppRuntime(streams + query);
        try {
            siddhiAppRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    log.info("output event: ");
                    EventPrinter.print(events);
                }
            });
            InputHandler cseEventStreamHandler =
                    siddhiAppRuntime.getInputHandler("cseEventStream");
            InputHandler twitterStreamHandler =
                    siddhiAppRuntime.getInputHandler("twitterStream");
            siddhiAppRuntime.start();

            cseEventStreamHandler.send(new Object[]{"WSO2", 700f, 14});
            cseEventStreamHandler.send(new Object[]{"ABC", 60.5f, 2});

            twitterStreamHandler.send(new Object[]{100, "Hello World", "XXX"});
            twitterStreamHandler.send(new Object[]{101, "Hello SIDDHI", "WSO2"});

            cseEventStreamHandler.send(new Object[]{"WSO2", 900f, 14});
            cseEventStreamHandler.send(new Object[]{"ABC", 60.5f, 2});

        } finally {
            siddhiAppRuntime.shutdown();
        }

    }
}
