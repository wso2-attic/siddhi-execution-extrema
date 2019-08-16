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


import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.event.stream.populater.ComplexEventPopulater;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.extension.siddhi.execution.extrema.util.ExtremaCalculator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * The kalmanMinMax function uses the kalman filter to smooth the time series values in the given
 * window size, and then determine the maxima and minima of that set of values.
 */
@Extension(
        name = "kalmanMinMax",
        namespace = "extrema",
        description = "`kalmanMinMax` uses the Kalman filter to smooth the values of the time series in the given " +
                "window size, and then determines the maxima and minima of that set of values." +
                " It returns the events with the minimum and/or maximum values for the specified attribute" +
                " within the given window length, with the extrema type as `min` or `max` as relevant.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the minimum and/or maximum value is required.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE}),
                @Parameter(name = "q",
                        description = "The standard deviation of the process noise.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "r",
                        description = "The standard deviation of the measurement noise.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "window.size",
                        description = "The length of the window within which the minimum and/or the " +
                                "maximum value for the given window should be identified.",
                        type = {DataType.INT}),
                @Parameter(name = "extrema.type",
                        description = "This can be `min`, `max` or `minmax`.\n" +
                                "`min`: If this is specified, minimum values are identified within the " +
                                "given window length, and they are returned with `min` as their extrema type.\n" +
                                "`max`: If this is specified, maximum values are identified within the given " +
                                "window length, and they are returned with `max` as their extrema type.\n" +
                                "`minmax`: If this is specified, both minimum and maximum values are identified " +
                                "within the given window length and returned. The extrema type is specified as `min`" +
                                " for the minimum events, and as `max` for the maximum events.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (item string, price long);\n" +
                                "\n" +
                                "from InputStream#extrema:kalmanMinMax(price, 0.000001,0.0001, 25, 'min')" +
                                "insert all events into OutputStream;",
                        description =  "This function returns the minimum value of a set of price values, to the " +
                                "output stream."
                )
        }
)
public class KalmanMinMaxStreamProcessor extends StreamProcessor<KalmanMinMaxStreamProcessor.ExtensionState> {

    private ExtremaType extremaType;
    private ExtremaCalculator extremaCalculator = null;
    private int windowSize = 0;
    private double q;
    private double r;
    private int minEventPos;
    private int maxEventPos;
    private StreamEventCloner streamEventCloner;

    private List<Attribute> attributeList = new ArrayList<>();

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           ExtensionState state) {
        this.streamEventCloner = streamEventCloner;
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        synchronized (this) {
            while (streamEventChunk.hasNext()) {

                StreamEvent event = streamEventChunk.next();
                streamEventChunk.remove();
                Double eventKey = (Double) attributeExpressionExecutors[0].execute(event);
                extremaCalculator = new ExtremaCalculator(q, r);
                state.eventStack.add(event);
                state.valueStack.add(eventKey);

                if (state.eventStack.size() > windowSize) {

                    Queue<Double> output = extremaCalculator.kalmanFilter(state.valueStack);
                    StreamEvent maximumEvent;
                    StreamEvent minimumEvent;

                    switch (extremaType) {
                        case MINMAX:
                            maximumEvent = getMaxEvent(output, state);
                            minimumEvent = getMinEvent(output, state);
                            if (maximumEvent != null && minimumEvent != null) {
                                if (maxEventPos > minEventPos) {
                                    returnEventChunk.add(minimumEvent);
                                    returnEventChunk.add(maximumEvent);
                                } else {
                                    returnEventChunk.add(maximumEvent);
                                    returnEventChunk.add(minimumEvent);
                                }
                            } else if (maximumEvent != null) {
                                returnEventChunk.add(maximumEvent);
                            } else if (minimumEvent != null) {
                                returnEventChunk.add(minimumEvent);
                            }
                            break;
                        case MIN:
                            minimumEvent = getMinEvent(output, state);
                            if (minimumEvent != null) {
                                returnEventChunk.add(minimumEvent);
                            }
                            break;
                        case MAX:
                            maximumEvent = getMaxEvent(output, state);
                            if (maximumEvent != null) {
                                returnEventChunk.add(maximumEvent);
                            }
                            break;
                    }
                    state.eventStack.remove();
                    state.valueStack.remove();
                }
            }
        }
        if (returnEventChunk.getFirst() != null) {
            nextProcessor.process(returnEventChunk);
        }
    }

    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent,
                                                AbstractDefinition inputDefinition,
                                                ExpressionExecutor[] attributeExpressionExecutors,
                                                ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 5) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to " +
                    "KalmanMinMaxStreamProcessor, required 5, but found " + attributeExpressionExecutors.length);
        }

        if (!(attributeExpressionExecutors[0].getReturnType() == Attribute.Type.DOUBLE
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.FLOAT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG)) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 1st " +
                    "argument of KalmanMinMaxStreamProcessor, required " + Attribute.Type.DOUBLE +
                    " or " + Attribute.Type.FLOAT + " or " + Attribute.Type.INT + " or " +
                    Attribute.Type.LONG + " but found " + attributeExpressionExecutors[0].getReturnType().toString());
        }

        try {
            q = Double.parseDouble(String.valueOf(((ConstantExpressionExecutor)
                    attributeExpressionExecutors[1]).getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 2nd argument " +
                    "of KalmanMinMaxStreamProcessor  required " + Attribute.Type.DOUBLE + " constant, " +
                    "but found " + attributeExpressionExecutors[1].getReturnType().toString());
        }
        try {
            r = Double.parseDouble(String.valueOf(((ConstantExpressionExecutor)
                    attributeExpressionExecutors[2]).getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 3rd argument of " +
                    "KalmanMinMaxStreamProcessor required " + Attribute.Type.DOUBLE + " constant, but found " +
                    attributeExpressionExecutors[2].getReturnType().toString());
        }
        try {
            windowSize = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor)
                    attributeExpressionExecutors[3]).getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 4th argument of " +
                    "KalmanMinMaxStreamProcessor required " + Attribute.Type.INT + " constant, but found " +
                    attributeExpressionExecutors[3].getReturnType().toString());
        }

        String extremeType = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue();

        if ("min".equalsIgnoreCase(extremeType)) {
            extremaType = ExtremaType.MIN;
        } else if ("max".equalsIgnoreCase(extremeType)) {
            extremaType = ExtremaType.MAX;
        } else {
            extremaType = ExtremaType.MINMAX;
        }

        LinkedList<StreamEvent> eventStack = new LinkedList<StreamEvent>();
        Queue<Double> valueStack = new LinkedList<Double>();
        Queue<StreamEvent> uniqueQueue = new LinkedList<StreamEvent>();

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("extremaType", Attribute.Type.STRING));
        this.attributeList = attributeList;
        return () -> new ExtensionState(eventStack, valueStack, uniqueQueue);

    }

    private StreamEvent getMinEvent(Queue<Double> output, ExtensionState state) {
        // value 2 is an optimized value for stock market domain, this value may change for other domains
        Integer smoothenedMinEventPosition = extremaCalculator.findMin(output, 2);
        if (smoothenedMinEventPosition != null) {
            //value 10 is an optimized value for stock market domain, this value may change for other domains
            Integer minEventPosition = extremaCalculator.findMin(state.valueStack, 10);
            if (minEventPosition != null) {
                StreamEvent returnMinimumEvent = getExtremaEvent(minEventPosition, state);
                if (returnMinimumEvent != null) {
                    minEventPos = minEventPosition;
                    complexEventPopulater.populateComplexEvent(returnMinimumEvent, new Object[]{"min"});
                    return returnMinimumEvent;
                }
            }
        }
        return null;
    }

    private StreamEvent getMaxEvent(Queue<Double> output, ExtensionState state) {
        // value 2 is an optimized value for stock market domain, this value may change for other domains
        Integer smoothenedMaxEventPosition = extremaCalculator.findMax(output, 2);
        if (smoothenedMaxEventPosition != null) {
            //value 10 is an optimized value for stock market domain, this value may change for other domains
            Integer maxEventPosition = extremaCalculator.findMax(state.valueStack, 10);
            if (maxEventPosition != null) {
                StreamEvent returnMaximumEvent = getExtremaEvent(maxEventPosition, state);
                if (returnMaximumEvent != null) {
                    maxEventPos = maxEventPosition;
                    complexEventPopulater.populateComplexEvent(returnMaximumEvent, new Object[]{"max"});
                    return returnMaximumEvent;
                }
            }
        }
        return null;
    }

    private StreamEvent getExtremaEvent(Integer eventPosition, ExtensionState state) {
        StreamEvent extremaEvent = state.eventStack.get(eventPosition);
        if (!state.uniqueQueue.contains(extremaEvent)) {
            //value 5 is an optimized value for stock market domain, this value may change for other domains
            if (state.uniqueQueue.size() > 5) {
                state.uniqueQueue.remove();
            }
            state.uniqueQueue.add(extremaEvent);
            return streamEventCloner.copyStreamEvent(extremaEvent);
        }
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return this.attributeList;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    /**
     * Enum for Extrema Type.
     * Extrema Type indicates whether the returned value is a min value or a max value.
     */
    public enum ExtremaType {
        MIN, MAX, MINMAX
    }

    static class ExtensionState extends State {
        private static final String EVENT_STACK = "eventStack";
        private static final String VALUE_STACK = "valueStack";
        private static final String UNIQUE_QUEUE = "uniqueQueue";

        private LinkedList<StreamEvent> eventStack;
        private Queue<Double> valueStack;
        private Queue<StreamEvent> uniqueQueue;

        public ExtensionState(LinkedList<StreamEvent> eventStack, Queue<Double> valueStack,
                               Queue<StreamEvent> uniqueQueue) {
            this.eventStack = eventStack;
            this.valueStack = valueStack;
            this.uniqueQueue = uniqueQueue;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            synchronized (this) {
                return new HashMap<String, Object>() {
                    {
                        put(EVENT_STACK, eventStack);
                        put(VALUE_STACK, valueStack);
                        put(UNIQUE_QUEUE, uniqueQueue);
                    }
                };
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void restore(Map<String, Object> state) {
            synchronized (this) {
                eventStack = (LinkedList<StreamEvent>) state.get(EVENT_STACK);
                valueStack = (Queue<Double>) state.get(VALUE_STACK);
                uniqueQueue = (Queue<StreamEvent>) state.get(UNIQUE_QUEUE);
            }
        }
    }
}
