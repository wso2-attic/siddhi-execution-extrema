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
import io.siddhi.core.executor.VariableExpressionExecutor;
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
 * kernalMinMax uses Gaussian Kernel to smooth the time series values in the given window size,
 * and then determine the maxima and minima of that set of values.
 */
@Extension(
        name = "kernelMinMax",
        namespace = "extrema",
        description = "`kernelMinMax` uses Gaussian Kernel to smooth values of the series within the given" +
                " window size, and then determines the maxima and minima of that set of values. " +
                "It returns the events with the minimum and/or maximum values for the specified attribute " +
                "within the given window length, with the extrema type as `min` or `max` as relevant.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the minimum and/or maximum value is required.",
                        type = {DataType.INT, DataType.FLOAT, DataType.DOUBLE, DataType.LONG}),
                @Parameter(name = "bandwidth",
                        description = "The bandwidth of the Gaussian Kernel calculation.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "window.size",
                        description = "The length of the window within which the minimum and/or the " +
                                "maximum value for the given window should be identified.",
                        type = {DataType.INT}),
                @Parameter(name = "extrema.type",
                        description = "This parameter can denote the minimum value, maximum value or " +
                                "both minimum and maximum values.\n" +
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
                                "from InputStream#extrema:kernelMinMax(price, 3, 7, ‘max’)\n" +
                                "select *\n" +
                                "insert into OutputStream;",
                        description =  "This query returns the maximum values for a set of price values while the " +
                                "bandwidth of the Gaussian Kernel calculation is three and the length of the " +
                                "window is seven."
                )
        }
)
public class KernelMinMaxStreamProcessor extends StreamProcessor<KernelMinMaxStreamProcessor.ExtensionState> {

    private ExtremaType extremaType;
    private int[] variablePosition;
    private double bw = 0;
    private int windowSize = 0;
    private ExtremaCalculator extremaCalculator = null;
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
                Double eventKey = (Double) event.getAttribute(variablePosition);
                state.eventStack.add(event);
                state.valueStack.add(eventKey);

                if (state.eventStack.size() > windowSize) {
                    Queue<Double> smoothedValues = extremaCalculator.smooth(state.valueStack, bw);
                    StreamEvent minimumEvent;
                    StreamEvent maximumEvent;

                    switch (extremaType) {
                        case MINMAX:
                            maximumEvent = getMaxEvent(smoothedValues, state);
                            minimumEvent = getMinEvent(smoothedValues, state);
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
                            minimumEvent = getMinEvent(smoothedValues, state);
                            if (minimumEvent != null) {
                                returnEventChunk.add(minimumEvent);
                            }
                            break;
                        case MAX:
                            maximumEvent = getMaxEvent(smoothedValues, state);
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

        if (attributeExpressionExecutors.length != 4) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to " +
                    "KernelMinMaxStreamProcessor, required 4, but found "
                    + attributeExpressionExecutors.length);
        }
        if (!(attributeExpressionExecutors[0].getReturnType() == Attribute.Type.DOUBLE
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.FLOAT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG)) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the " +
                    "1st argument of KernelMinMaxStreamProcessor, required " + Attribute.Type.DOUBLE +
                    " or " + Attribute.Type.FLOAT + " or " + Attribute.Type.INT + " or " +
                    Attribute.Type.LONG + " but found "
                    + attributeExpressionExecutors[0].getReturnType().toString());
        }

        try {
            bw = Double.parseDouble(String.valueOf(((ConstantExpressionExecutor)
                    attributeExpressionExecutors[1]).getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 2nd argument of " +
                    "KernelMinMaxStreamProcessor required " + Attribute.Type.DOUBLE + " constant, but found " +
                    attributeExpressionExecutors[1].getReturnType().toString());
        }

        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)
                || attributeExpressionExecutors[2].getReturnType() != Attribute.Type.INT) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 3rd argument " +
                    "of KernelMinMaxStreamProcessor, required " + Attribute.Type.INT + " constant, but found " +
                    attributeExpressionExecutors[2].getReturnType().toString());
        }
        if (!(attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor)
                || attributeExpressionExecutors[3].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the 4th argument " +
                    "of KernelMinMaxStreamProcessor, required " + Attribute.Type.STRING + " constant, but found " +
                    attributeExpressionExecutors[2].getReturnType().toString());
        }

        variablePosition = ((VariableExpressionExecutor) attributeExpressionExecutors[0]).getPosition();
        windowSize = Integer.parseInt(String.valueOf(
                ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
        String extremeType = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue();

        if ("min".equalsIgnoreCase(extremeType)) {
            extremaType = ExtremaType.MIN;
        } else if ("max".equalsIgnoreCase(extremeType)) {
            extremaType = ExtremaType.MAX;
        } else {
            extremaType = ExtremaType.MINMAX;
        }
        extremaCalculator = new ExtremaCalculator();
        LinkedList<StreamEvent> eventStack = new LinkedList<StreamEvent>();
        Queue<Double> valueStack = new LinkedList<Double>();
        Queue<StreamEvent> uniqueQueue = new LinkedList<StreamEvent>();

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("extremaType", Attribute.Type.STRING));
        this.attributeList = attributeList;
        return () -> new ExtensionState(eventStack, valueStack, uniqueQueue);

    }

    private StreamEvent getMaxEvent(Queue<Double> smoothedValues, ExtensionState state) {
        //value 1 is an optimized value for stock market domain, this value may change for other domains
        Integer maxPosition = extremaCalculator.findMax(smoothedValues, 1);
        if (maxPosition != null) {
            //values 5 and 3 are optimized values for stock market domain, these value may change for other domains
            Integer maxEventPosition = extremaCalculator.findMax(
                    state.valueStack, windowSize / 5, windowSize / 3);
            StreamEvent returnMaximumEvent = getExtremaEvent(maxPosition, maxEventPosition, state);
            if (returnMaximumEvent != null) {
                maxEventPos = maxEventPosition;
                complexEventPopulater.populateComplexEvent(returnMaximumEvent, new Object[]{"max"});
                return returnMaximumEvent;
            }
        }
        return null;
    }

    private StreamEvent getMinEvent(Queue<Double> smoothedValues, ExtensionState state) {
        //value 1 is an optimized value for stock market domain, this value may change for other domains
        Integer minPosition = extremaCalculator.findMin(smoothedValues, 1);
        if (minPosition != null) {
            //values 5 and 3 are optimized values for stock market domain, these value may change for other domains
            Integer minEventPosition = extremaCalculator.findMin(
                    state.valueStack, windowSize / 5, windowSize / 3);
            StreamEvent returnMinimumEvent = getExtremaEvent(minPosition, minEventPosition, state);
            if (returnMinimumEvent != null) {
                minEventPos = minEventPosition;
                complexEventPopulater.populateComplexEvent(returnMinimumEvent, new Object[]{"min"});
                return returnMinimumEvent;
            }
        }
        return null;
    }

    private StreamEvent getExtremaEvent(Integer smoothenedPosition, Integer eventPosition, ExtensionState state) {
        //values 5 and 3 are optimized values for stock market domain, these value may change for other domains
        if (eventPosition != null && eventPosition - smoothenedPosition <= windowSize / 5
                && smoothenedPosition - eventPosition <= windowSize / 2) {
            StreamEvent extremaEvent = state.eventStack.get(eventPosition);
            if (!state.uniqueQueue.contains(extremaEvent)) {
                //value 5 is an optimized value for stock market domain, this value may change for other domains
                if (state.uniqueQueue.size() > 5) {
                    state.uniqueQueue.remove();
                }
                state.uniqueQueue.add(extremaEvent);
                state.eventStack.remove();
                state.valueStack.remove();
                return streamEventCloner.copyStreamEvent(extremaEvent);
            }
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
     */
    public enum ExtremaType {
        MIN, MAX, MINMAX
    }

    static class ExtensionState extends State {
        private static final String EVENT_STACK = "eventStack";
        private static final String VALUE_STACK = "valueStack";
        private static final String UNIQUE_QUEUE = "uniqueQueue";

        LinkedList<StreamEvent> eventStack;
        Queue<Double> valueStack;
        Queue<StreamEvent> uniqueQueue;

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
