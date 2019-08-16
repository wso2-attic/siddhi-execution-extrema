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

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class finds minimum and/or maximum value within a given length window (maxPreBound+maxPostBound),
 * where following conditions are met.
 * <p>
 * For minimum:
 * - an event at least preBoundChange% greater than minimum event must have arrived prior to minimum,
 * within maxPreBound length window
 * - an event at least postBoundChange% greater than minimum event must arrive after minimum,
 * within maxPostBound length window
 * <p>
 * For maximum:
 * - an event at least preBoundChange% less than maximum event must have arrived prior to maximum,
 * within maxPreBound length window
 * - an event at least postBoundChange% less than maximum event must arrive after maximum,
 * within maxPostBound length window
 * <p>
 * Sample Query (variable, maxPreBound, maxPostBound, preBoundChange, postBoundChange, extrema type):
 * from inputStream#timeseries:minMax(price, 4, 4, 1, 2, 'minmax')
 * select *
 * insert into outputStream;
 */
@Extension(
        name = "minMax",
        namespace = "extrema",
        description = "`minMax` finds the minimum and/or the maximum value within a given length window" +
                " (maxPreBound+maxPostBound), where following conditions are met. \n" +
                "\n" +
                "For minimum: \n" +
                "An event where the value for the specified attribute is greater by the percentage " +
                "specified as the `preBoundChange` must have arrived within the `maxPreBound` length window before" +
                " the event with the minimum value.\n" +
                "An event where the value for the specified attribute is greater by the percentage " +
                "specified as the `postBoundChange` must have arrived within the `maxPostBound` length window after" +
                " the event with the minimum value.\n" +
                "\n" +
                "For maximum: \n" +
                "An event where the value for the specified attribute is less by the percentage specified" +
                " as the `preBoundChange` must have arrived within the `maxPreBound` length window before the event" +
                " with the maximum value.\n" +
                "An event where the value for the specified attribute is less by the percentage specified" +
                " as the `postBoundChange` must have arrived within the `maxPreBound` length window after the event" +
                " with the maximum value.\n" +
                "\n" +
                "The extension returns the events with the minimum and/or maximum for the specified attribute within" +
                " the given window length, with the extrema type as min or max as relevant. " +
                "These events are returned with the following additional parameters.\n" +
                "`preBound`: The actual distance between the minimum/maximum value and the threshold value." +
                " This value must be within the `MaxPreBound` window.\n" +
                "postBound: The actual distance between the minimum/maximum value and the threshold value. " +
                "This value must be within the `MaxPostBound` window.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the minimum and/or the maximum value is required.",
                        type = {DataType.INT, DataType.FLOAT, DataType.DOUBLE, DataType.LONG}),
                @Parameter(name = "max.pre.bound",
                        description = "The maximum pre window length to be considered (before the min/max event).",
                        type = {DataType.INT}),
                @Parameter(name = "max.post.bound",
                        description = "The maximum post window length to be considered (after the min/max event).",
                        type = {DataType.INT}),
                @Parameter(name = "pre.bound.change",
                        description = "The threshold value for  the percentage difference between the value that " +
                                "occurred in the `maxPreBound` length window before the maximum value, and the " +
                                "maximum value.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "post.bound.change",
                        description = "The threshold value for  the percentage difference between the value that " +
                                "occurred in the `maxPreBound` length window after the maximum value, and the " +
                                "maximum value.",
                        type = {DataType.DOUBLE}),
                @Parameter(name = "extrema.type",
                        description = "This can be min, max or minmax.\n" +
                                "`min`: If this is specified, minimum values are identified within the " +
                                "given window length, and they are returned with `min` as their extrema type.\n" +
                                "`max`: If this is specified, maximum values are identified within the " +
                                "given window length, and they are returned with `max` as their extrema type.\n" +
                                "`minmax`: If this is specified, both minimum and maximum values are " +
                                "identified within the given window length and returned. The extrema " +
                                "type is specified as `min` for the minimum events, and as `max` for the " +
                                "maximum events.",
                        type = {DataType.STRING})
        },
        examples = {
                @Example(
                        syntax = "from InputStream#extrema:minMax(price, 4, 4, 1, 2, 'max')\n" +
                                "select *\n" +
                                "insert into OutputStream; ",
                        description =  "This query returns the maximum values found within a set of price values."
                ),
                @Example(
                        syntax = "from InputStream#extrema:minMax(price, 4, 4, 1, 2, 'min')\n" +
                                "select *\n" +
                                "insert into OutputStream; ",
                        description =  "This query returns the minimum values found within a set of price values."
                ),
                @Example(
                        syntax = "from InputStream#extrema:minMax(price, 4, 4, 1, 2, 'minmax')\n" +
                                "select *\n" +
                                "insert into OutputStream; ",
                        description = "This query returns both the minimum values and the maximum values found " +
                                "within a set of price values."
                )
        }
)
public class MinMaxStreamProcessor extends StreamProcessor<MinMaxStreamProcessor.ExtensionState> {
    private ExtremaType extremaType; // Whether to find min and/or max
    private int maxPreBound; // maxPreBound window length
    private int maxPostBound; // maxPostBound window length
    private double preBoundChange; // preBoundChange percentage
    private double postBoundChange; // postBoundChange percentage
    private List<Attribute> attributeList = new ArrayList<>();

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           ExtensionState state) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                streamEventChunk.remove();

                // Variable value of the latest event
                double value = ((Number) attributeExpressionExecutors[0].execute(event)).doubleValue();
                state.eventStack.add(event);

                // Create an object holding latest event value and insert into valueStack and valueStackLastL
                AttributeDetails newInput = new AttributeDetails();
                newInput.setValue(value);
                state.valueStack.add(newInput);

                switch (extremaType) {
                    case MINMAX:
                        // Retain only maxPreBound+maxPostBound events
                        if (state.eventStack.size() > maxPreBound + maxPostBound) {
                            state.eventStack.remove();
                            state.valueRemoved = state.valueStack.remove();
                        }
                        state.currentMax = maxDequeIterator(newInput, state);
                        state.currentMin = minDequeIterator(newInput, state);
                        // Find whether current max satisfies preBoundChange, postBoundChange conditions
                        // and output value if so
                        if (newInput.getValue() <= state.currentMax.getMaxThreshold()
                                && !state.currentMax.isOutputAsRealMax()
                                && state.currentMax.isEligibleForRealMax()) {
                            StreamEvent returnEvent = findIfActualMax(newInput, state);
                            if (returnEvent != null) {
                                returnEventChunk.add(returnEvent);
                            }
                        }
                        // Find whether current min satisfies preBoundChange, postBoundChange conditions
                        // and output value if so
                        if (newInput.getValue() >= state.currentMin.getMinThreshold()
                                && !state.currentMin.isOutputAsRealMin()
                                && state.currentMin.isEligibleForRealMin()) {
                            StreamEvent returnEvent = findIfActualMin(newInput, state);
                            if (returnEvent != null) {
                                returnEventChunk.add(returnEvent);
                            }
                        }
                        break;
                    case MAX:
                        // Retain only maxPreBound+maxPostBound events
                        if (state.eventStack.size() > maxPreBound + maxPostBound) {
                            state.eventStack.remove();
                            state.valueRemoved = state.valueStack.remove();
                        }
                        state.currentMax = maxDequeIterator(newInput, state);
                        // Find whether current max satisfies preBoundChange, postBoundChange conditions
                        // and output value if so
                        if (newInput.getValue() <= state.currentMax.getMaxThreshold()
                                && !state.currentMax.isOutputAsRealMax()
                                && state.currentMax.isEligibleForRealMax()) {
                            StreamEvent returnEvent = findIfActualMax(newInput, state);
                            if (returnEvent != null) {
                                returnEventChunk.add(returnEvent);
                            }
                        }
                        break;
                    case MIN:
                        // Retain only maxPreBound+maxPostBound events
                        if (state.eventStack.size() > maxPreBound + maxPostBound) {
                            state.eventStack.remove();
                            state.valueRemoved = state.valueStack.remove();
                        }
                        state.currentMin = minDequeIterator(newInput, state);
                        // Find whether current min satisfies preBoundChange, postBoundChange conditions
                        // and output value if so
                        if (newInput.getValue() >= state.currentMin.getMinThreshold()
                                && !state.currentMin.isOutputAsRealMin()
                                && state.currentMin.isEligibleForRealMin()) {
                            StreamEvent returnEvent = findIfActualMin(newInput, state);
                            if (returnEvent != null) {
                                returnEventChunk.add(returnEvent);
                            }
                        }
                        break;
                }
            }
        }
        nextProcessor.process(returnEventChunk);
    }

    /**
     * The init method of MinMaxStreamProcessor,
     * this method will be called before other methods
     * <p>
     * Input parameters:
     * 1st parameter: variable
     * 2nd parameter: maxPreBound window length
     * 3rd parameter: maxPostBound window length
     * 4th parameter: preBoundChange percentage
     * 5th parameter: postBoundChange percentage
     * 6th parameter: extrema type
     * <p>
     * Additional output attributes:
     * extremaType
     * preBound, postBound: distance from min/max to where the threshold values occur (preBoundChange%, postBoundChange%
     * values)
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param siddhiQueryContext         the context of the siddhi query
     * @return the additional output attributes (extremaType, preBound, postBound) introduced by the function
     */
    @Override
    protected StateFactory<ExtensionState> init(MetaStreamEvent metaStreamEvent,
                                                AbstractDefinition inputDefinition,
                                                ExpressionExecutor[] attributeExpressionExecutors,
                                                ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean outputExpectsExpiredEvents, boolean findToBeExecuted,
                                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 6) {
            throw new SiddhiAppValidationException(
                    "Invalid no of arguments passed to MinMaxStreamProcessor, required 6, but found "
                            + attributeExpressionExecutors.length);
        }
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new SiddhiAppValidationException("MinMaxStreamProcessor's 1st parameter should"
                    + " be a variable, but found " + attributeExpressionExecutors[0].getClass());
        }
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Constant value expected as the 2nd parameter (maxPreBound)"
                    + " but found " + attributeExpressionExecutors[1].getClass());
        }
        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Constant value expected as the 3rd parameter (maxPostBound)"
                    + " but found " + attributeExpressionExecutors[2].getClass());
        }
        if (!(attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Constant value expected as the 4th parameter (preBoundChange)"
                    + " but found " + attributeExpressionExecutors[3].getClass());
        }
        if (!(attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Constant value expected as the 5th parameter (postBoundChange)"
                    + " but found " + attributeExpressionExecutors[4].getClass());
        }
        if (!(attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("Constant value expected as the 6th parameter (extrema type)"
                    + " but found " + attributeExpressionExecutors[5].getClass());
        }
        if (!(attributeExpressionExecutors[0].getReturnType() == Attribute.Type.DOUBLE
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.FLOAT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG)) {
            throw new SiddhiAppValidationException(
                    "Invalid parameter type found for the 1st argument (variable) of MinMaxStreamProcessor, "
                            + "required " + Attribute.Type.DOUBLE + " or " + Attribute.Type.FLOAT + " or "
                            + Attribute.Type.INT + " or " + Attribute.Type.LONG + " but found "
                            + attributeExpressionExecutors[0].getReturnType().toString());
        }
        try {
            maxPreBound = Integer.parseInt(
                    String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException(
                    "Invalid parameter type found for the 2nd argument (maxPreBound) of MinMaxStreamProcessor "
                            + "required " + Attribute.Type.INT + " constant, but found "
                            + attributeExpressionExecutors[1].getReturnType().toString());
        }
        try {
            maxPostBound = Integer.parseInt(
                    String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException(
                    "Invalid parameter type found for the 3rd argument (maxPostBound) of MinMaxStreamProcessor "
                            + "required " + Attribute.Type.INT + " constant, but found "
                            + attributeExpressionExecutors[2].getReturnType().toString());
        }
        if (maxPreBound == 0 && maxPostBound == 0) {
            throw new SiddhiAppValidationException("Both post bound limit and pre bound limit cannot be 0. " +
                    "At least one must have a positive integer value");
        }
        try {
            preBoundChange = Double.parseDouble(
                    String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException(
                    "Invalid parameter type found for the 4th argument (preBoundChange) of MinMaxStreamProcessor "
                            + "required " + Attribute.Type.DOUBLE + " constant, but found "
                            + attributeExpressionExecutors[3].getReturnType().toString());
        }
        if (maxPreBound == 0) {
            if (preBoundChange != 0) {
                throw new SiddhiAppValidationException("When pre bound limit is 0, the pre bound change percentage "
                        + "should also be 0, but found " + preBoundChange);
            }
        }
        try {
            postBoundChange = Double.parseDouble(
                    String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue()));
        } catch (NumberFormatException e) {
            throw new SiddhiAppValidationException(
                    "Invalid parameter type found for the 5th argument (postBoundChange) of MinMaxStreamProcessor "
                            + "required " + Attribute.Type.DOUBLE + " constant, but found "
                            + attributeExpressionExecutors[4].getReturnType().toString());
        }
        if (maxPostBound == 0) {
            if (postBoundChange != 0) {
                throw new SiddhiAppValidationException(
                        "When post bound limit is 0, the post bound change percentage " + "should also be 0, but found "
                                + postBoundChange);
            }
        }
        String extremaType = ((String) ((ConstantExpressionExecutor) attributeExpressionExecutors[5]).getValue())
                .trim();
        if ("min".equalsIgnoreCase(extremaType)) {
            this.extremaType = ExtremaType.MIN;
        } else if ("max".equalsIgnoreCase(extremaType)) {
            this.extremaType = ExtremaType.MAX;
        } else if ("minmax".equalsIgnoreCase(extremaType)) {
            this.extremaType = ExtremaType.MINMAX;
        } else {
            throw new SiddhiAppValidationException("Only 'min', 'max' and 'minmax' values are accepted as "
                    + "extrema type, but found value " + extremaType);
        }

        LinkedList<StreamEvent> eventStack = new LinkedList<StreamEvent>();
        LinkedList<AttributeDetails> valueStack = new LinkedList<AttributeDetails>();

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("extremaType", Attribute.Type.STRING));
        attributeList.add(new Attribute("preBound", Attribute.Type.INT));
        attributeList.add(new Attribute("postBound", Attribute.Type.INT));
        this.attributeList = attributeList;
        return () -> new ExtensionState(eventStack, valueStack);
    }

    /**
     * Method to find whether a value preBoundChange% greater than or equal to min exists within maxPreBound length
     * window, by looping through older events.
     * It further verifies whether postBoundChange condition is met within maxPostBound window.
     * This method is called only if
     * the latest event satisfies the postBoundChange condition &&
     * current min has not already been sent as output &&
     * current min has not failed preBoundChange, maxPostBound condition previously
     *
     * @param latestEvent object holding value of latest event
     * @return if preBoundChange, maxPostBound conditions are met, send stream event output with
     * extrema type,
     * preBound (distance at which a value satisfying preBoundChange condition is found),
     * postBound (distance at which a value satisfying postBoundChange condition is found)
     */
    private StreamEvent findIfActualMin(AttributeDetails latestEvent, ExtensionState state) {
        int indexCurrentMin = state.valueStack.indexOf(state.currentMin);
        int postBound = state.valueStack.indexOf(latestEvent) - indexCurrentMin;
        // If latest event is at a distance greater than maxPostBound from min, min is not eligible to be sent as output
        if (postBound > maxPostBound) {
            state.currentMin.notEligibleForRealMin();
            return null;
        }
        // If maxPreBound is 0, no need to check preBoundChange. Send output with postBound value
        if (maxPreBound == 0) {
            StreamEvent outputEvent = state.eventStack.get(indexCurrentMin);
            complexEventPopulater.populateComplexEvent(outputEvent, new Object[]{"min", 0, postBound});
            state.currentMin.sentOutputAsRealMin();
            return outputEvent;
        }
        int preBound = 1;
        double dThreshold = state.currentMin.getValue() + state.currentMin.getValue() * preBoundChange / 100;
        while (preBound <= maxPreBound && indexCurrentMin - preBound >= 0) {
            if (state.valueStack.get(indexCurrentMin - preBound).getValue() >= dThreshold) {
                StreamEvent outputEvent = state.eventStack.get(indexCurrentMin);
                complexEventPopulater.populateComplexEvent(outputEvent, new Object[]{"min", preBound, postBound});
                state.currentMin.sentOutputAsRealMin();
                return outputEvent;
            }
            ++preBound;
        }
        // Completed iterating through maxPreBound older events. No events which satisfy preBoundChange condition found.
        // Therefore min is not eligible to be sent as output.
        state.currentMin.notEligibleForRealMin();
        return null;
    }

    /**
     * Method to find whether a value preBoundChange% less than or equal to max exists within maxPreBound length window,
     * by looping through older events.
     * It further verifies whether postBoundChange condition is met within maxPostBound window.
     * This method is called only if
     * the latest event satisfies the postBoundChange condition &&
     * current max has not already been sent as output &&
     * current max has not failed preBoundChange, maxPostBound condition previously
     *
     * @param latestEvent object holding value of latest event
     * @return if preBoundChange, maxPostBound conditions are met, send stream event output with
     * extrema type,
     * preBound (distance at which a value satisfying preBoundChange condition is found),
     * postBound (distance at which a value satisfying postBoundChange condition is found)
     */
    private StreamEvent findIfActualMax(AttributeDetails latestEvent, ExtensionState state) {
        int indexCurrentMax = state.valueStack.indexOf(state.currentMax);
        int postBound = state.valueStack.indexOf(latestEvent) - indexCurrentMax;
        // If latest event is at a distance greater than maxPostBound from max, max is not eligible to be sent as output
        if (postBound > maxPostBound) {
            state.currentMax.notEligibleForRealMax();
            return null;
        }
        // If maxPreBound is 0, no need to check preBoundChange. Send output with postBound value
        if (maxPreBound == 0) {
            StreamEvent outputEvent = state.eventStack.get(indexCurrentMax);
            complexEventPopulater.populateComplexEvent(outputEvent, new Object[]{"max", 0, postBound});
            state.currentMax.sentOutputAsRealMax();
            return outputEvent;
        }
        int preBound = 1;
        double dThreshold = state.currentMax.getValue() - state.currentMax.getValue() * preBoundChange / 100;
        while (preBound <= maxPreBound && indexCurrentMax - preBound >= 0) {
            if (state.valueStack.get(indexCurrentMax - preBound).getValue() <= dThreshold) {
                StreamEvent outputEvent = state.eventStack.get(indexCurrentMax);
                complexEventPopulater.populateComplexEvent(outputEvent, new Object[]{"max", preBound, postBound});
                state.currentMax.sentOutputAsRealMax();
                return outputEvent;
            }
            ++preBound;
        }
        // Completed iterating through maxPreBound older events. No events which satisfy preBoundChange condition found.
        // Therefore max is not eligible to be sent as output.
        state.currentMax.notEligibleForRealMax();
        return null;
    }

    /**
     * This method stores all the values possible to become next max, with current max (largest value)
     * at the head. The value expiring from maxPreBound + maxPostBound window is removed if it's in maxDeque
     *
     * @param valObject latest incoming value
     * @return maximum value (without checking preBoundChange, postBoundChange conditions)
     */
    private AttributeDetails maxDequeIterator(AttributeDetails valObject, ExtensionState state) {
        if (state.valueRemoved != null) {
            for (Iterator<AttributeDetails> iterator = state.maxDeque.descendingIterator(); iterator.hasNext(); ) {
                AttributeDetails possibleMaxValue = iterator.next();
                if (possibleMaxValue.getValue() < valObject.getValue()
                        || possibleMaxValue.getValue() <= state.valueRemoved.getValue()) {
                    if (possibleMaxValue.getValue() < valObject.getValue()) {
                        iterator.remove();
                    } else if (state.valueRemoved.equals(possibleMaxValue)) {
                        // If expired value is in maxDeque, it must be removed
                        iterator.remove();
                    }
                } else {
                    break;
                }
            }
        } else {
            for (Iterator<AttributeDetails> iterator = state.maxDeque.descendingIterator(); iterator.hasNext(); ) {
                if (iterator.next().getValue() < valObject.getValue()) {
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
        valObject.setMaxThreshold();
        state.maxDeque.addLast(valObject);
        return state.maxDeque.peek();
    }

    /**
     * This method stores all the values possible to become next min, with current min (minimum value)
     * at the head. The value expiring from maxPreBound + maxPostBound window is removed if it's in minDeque
     *
     * @param valObject latest incoming value
     * @return minimum value (without checking preBoundChange, postBoundChange conditions)
     */
    private AttributeDetails minDequeIterator(AttributeDetails valObject, ExtensionState state) {
        if (state.valueRemoved != null) {
            for (Iterator<AttributeDetails> iterator = state.minDeque.descendingIterator(); iterator.hasNext(); ) {
                AttributeDetails possibleMinValue = iterator.next();
                if (possibleMinValue.getValue() > valObject.getValue()
                        || possibleMinValue.getValue() >= state.valueRemoved.getValue()) {
                    if (possibleMinValue.getValue() > valObject.getValue()) {
                        iterator.remove();
                    } else if (state.valueRemoved.equals(possibleMinValue)) {
                        // If removed value is in minDeque, it must be removed
                        iterator.remove();
                    }
                } else {
                    break;
                }
            }
        } else {
            for (Iterator<AttributeDetails> iterator = state.minDeque.descendingIterator(); iterator.hasNext(); ) {
                if (iterator.next().getValue() > valObject.getValue()) {
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
        valObject.setMinThreshold();
        state.minDeque.addLast(valObject);
        return state.minDeque.peek();
    }

    /**
     * No resources to acquire. Therefore no method implementation
     */
    @Override
    public void start() {

    }

    /**
     * No resources to release. Therefore no method implementation
     */
    @Override
    public void stop() {

    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return this.attributeList;
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.SLIDE;
    }

    private enum ExtremaType {
        MIN, MAX, MINMAX
    }

    /*
     * The POJO class which holds additional information which is useful in finding
     * whether min/max is eligible to be sent as output
     */
    private class AttributeDetails {
        private double value; // Variable value
        private double minThreshold; // If event is min, the postBoundChange threshold to consider
        private double maxThreshold; // If event is max, the postBoundChange threshold to consider
        private boolean eligibleForRealMax = true; // Max eligibility based on preBoundChange, maxPostBound conditions.
        // Initially set to true
        private boolean eligibleForRealMin = true; // Min eligibility based on preBoundChange, maxPostBound conditions.
        private boolean outputAsRealMin = false; // Whether event was already sent as min output
        private boolean outputAsRealMax = false; // Whether event was already sent as max output

        /**
         * Method to return value
         *
         * @return object value
         */
        private double getValue() {
            return value;
        }

        /**
         * Method to set value
         *
         * @param value variable value
         */
        private void setValue(double value) {
            this.value = value;
        }

        /**
         * Method to set threshold value which satisfies postBoundChange condition, if this object becomes min
         */
        private void setMinThreshold() {
            minThreshold = value + value * postBoundChange / 100;
        }

        /**
         * Method to set threshold value which satisfies postBoundChange condition, if this object becomes max
         */
        private void setMaxThreshold() {
            maxThreshold = value - value * postBoundChange / 100;
        }

        /**
         * Method to return threshold. If this object becomes min, compare it's threshold with latest event
         *
         * @return threshold satisfying postBoundChange condition
         */
        private double getMinThreshold() {
            return minThreshold;
        }

        /**
         * Method to return threshold. If this object becomes max, compare it's threshold with latest event
         *
         * @return threshold satisfying postBoundChange condition
         */
        private double getMaxThreshold() {
            return maxThreshold;
        }

        /**
         * If preBoundChange condition is checked when this object is max, and it fails,
         * the object can be set as not eligible to be sent as max output.
         * Furthermore, if postBoundChange condition is not met within maxPostBound events same can be done.
         */
        private void notEligibleForRealMax() {
            eligibleForRealMax = false;
        }

        /**
         * If preBoundChange condition is checked when this object is min, and it fails,
         * the object can be set as not eligible to be sent as min output.
         * Furthermore, if postBoundChange condition is not met within maxPostBound events same can be done.
         */
        private void notEligibleForRealMin() {
            eligibleForRealMin = false;
        }

        /**
         * Method to return max eligibility based on preBoundChange, maxPostBound condition
         *
         * @return eligibility to become max
         */
        private boolean isEligibleForRealMax() {
            return eligibleForRealMax;
        }

        /**
         * Method to return min eligibility based on preBoundChange, maxPostBound condition
         *
         * @return eligibility to become min
         */
        private boolean isEligibleForRealMin() {
            return eligibleForRealMin;
        }

        /**
         * If this object was already sent as min output, set
         * outputAsRealMin = true
         */
        private void sentOutputAsRealMin() {
            outputAsRealMin = true;
        }

        /**
         * If this object was already sent as max output, set
         * outputAsRealMax = true
         */
        private void sentOutputAsRealMax() {
            outputAsRealMax = true;
        }

        /**
         * Check whether this object was already sent as min output. If so no need to check twice
         *
         * @return whether object was already sent as min output
         */
        private boolean isOutputAsRealMin() {
            return outputAsRealMin;
        }

        /**
         * Check whether this object was already sent as max output. If so no need to check twice
         *
         * @return whether object was already sent as max output
         */
        private boolean isOutputAsRealMax() {
            return outputAsRealMax;
        }
    }

    static class ExtensionState extends State {

        // Stores all the events within maxPreBound + maxPostBound window
        private LinkedList<StreamEvent> eventStack;
        // Stores all the values within maxPreBound + maxPostBound
        private LinkedList<AttributeDetails> valueStack;
        // window
        private AttributeDetails valueRemoved = null; // Expired event
        // Stores all the values which could
        private Deque<AttributeDetails> maxDeque = new LinkedList<AttributeDetails>();
        // Stores all the values which could
        private Deque<AttributeDetails> minDeque = new LinkedList<AttributeDetails>();
        // be next min (including current
        // min)
        private AttributeDetails currentMax = null; // Current max (before testing preBoundChange, postBoundChange
        // conditions)
        private AttributeDetails currentMin = null; // Current min (before testing preBoundChange, postBoundChange

        private ExtensionState(LinkedList<StreamEvent> eventStack, LinkedList<AttributeDetails> valueStack) {
            this.eventStack = eventStack;
            this.valueStack = valueStack;
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
                        put("eventStack", eventStack);
                        put("valueStack", valueStack);
                        put("maxDeque", maxDeque);
                        put("minDeque", minDeque);
                        put("valueRemoved", valueRemoved);
                        put("currentMax", currentMax);
                        put("currentMin", currentMin);
                    }
                };
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void restore(Map<String, Object> state) {
            synchronized (this) {
                eventStack = (LinkedList<StreamEvent>) state.get("eventStack");
                valueStack = (LinkedList<AttributeDetails>) state.get("valueStack");
                maxDeque = (Deque<AttributeDetails>) state.get("maxDeque");
                minDeque = (Deque<AttributeDetails>) state.get("minDeque");
                valueRemoved = (AttributeDetails) state.get("valueRemoved");
                currentMax = (AttributeDetails) state.get("currentMax");
                currentMin = (AttributeDetails) state.get("currentMin");
            }
        }
    }
}
