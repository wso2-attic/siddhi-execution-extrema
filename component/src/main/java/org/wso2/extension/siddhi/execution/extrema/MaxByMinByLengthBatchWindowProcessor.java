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

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.window.BatchingFindableWindowProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;
import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByConstants;
import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract class which gives the min/max event in a LengthBatch window
 */

public abstract class MaxByMinByLengthBatchWindowProcessor
        extends BatchingFindableWindowProcessor<MaxByMinByLengthBatchWindowProcessor.ExtensionState> {

    protected String minByMaxByExecutorType;
    private int length;
    private ExpressionExecutor minByMaxByExecutorAttribute;
    private SiddhiQueryContext siddhiQueryContext;
    private VariableExpressionExecutor[] variableExpressionExecutors;
    private MaxByMinByExecutor minByMaxByExecutor;
    private StreamEvent oldEvent;
    private StreamEvent expiredResultEvent;

    public MaxByMinByLengthBatchWindowProcessor() {

    }

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param siddhiQueryContext             the context of the siddhi query
     */
    @Override
    protected StateFactory<ExtensionState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                ConfigReader configReader,
                                                StreamEventClonerHolder streamEventClonerHolder,
                                                boolean outputExpectsExpiredEvents,
                                                boolean findToBeExecuted, SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        minByMaxByExecutor = new MaxByMinByExecutor();
        minByMaxByExecutor.setMinByMaxByExecutorType(minByMaxByExecutorType);

        if (this.attributeExpressionExecutors.length != 2) {
            throw new SiddhiAppValidationException(
                    "Invalid no of arguments passed to minbymaxby:" + minByMaxByExecutorType + " window, "
                            + "required 2, but found " + this.attributeExpressionExecutors.length);
        }

        Attribute.Type attributeType = this.attributeExpressionExecutors[0].getReturnType();
        if (!((attributeType == Attribute.Type.DOUBLE) || (attributeType == Attribute.Type.INT) || (attributeType
                == Attribute.Type.STRING) || (attributeType == Attribute.Type.FLOAT) || (attributeType
                == Attribute.Type.LONG))) {
            throw new SiddhiAppValidationException(
                    "Invalid parameter type found for the first argument of minbymaxby:" + minByMaxByExecutorType
                            + " window, " + "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG + " or "
                            + Attribute.Type.FLOAT + " or " + Attribute.Type.DOUBLE + "or" + Attribute.Type.STRING
                            + ", but found " + attributeType.toString());
        }
        attributeType = this.attributeExpressionExecutors[1].getReturnType();
        if (!((attributeType == Attribute.Type.LONG) || (attributeType == Attribute.Type.INT))) {
            throw new SiddhiAppValidationException(
                    "Invalid parameter type found for the second argument of minbymaxby:" + minByMaxByExecutorType
                            + " window, " + "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG
                            + ", but found " + attributeType.toString());
        }

        variableExpressionExecutors = new VariableExpressionExecutor[this.attributeExpressionExecutors.length - 1];
        variableExpressionExecutors[0] = (VariableExpressionExecutor) this.attributeExpressionExecutors[0];
        minByMaxByExecutorAttribute = variableExpressionExecutors[0];
        length = (Integer) (((ConstantExpressionExecutor) this.attributeExpressionExecutors[1]).getValue());


        return () -> new ExtensionState(null, null);

    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ExtensionState state) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = this.siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent currentEvent = streamEventCloner.copyStreamEvent(streamEvent);

                if (state.count == 0) {
                    outputStreamEventChunk.clear();
                    oldEvent = null;
                    //clonedResultEvent=resultEvent;
                }

                //Get the event which hold the minimum or maximum event

                if (minByMaxByExecutorType.equals(MaxByMinByConstants.MAX_BY)) {
                    state.resultEvent = MaxByMinByExecutor
                            .getMaxEventBatchProcessor(currentEvent, oldEvent, minByMaxByExecutorAttribute);
                    oldEvent = state.resultEvent;
                } else if (minByMaxByExecutorType.equals(MaxByMinByConstants.MIN_BY)) {
                    state.resultEvent = MaxByMinByExecutor
                            .getMinEventBatchProcessor(currentEvent, oldEvent, minByMaxByExecutorAttribute);
                    oldEvent = state.resultEvent;
                }

                state.count++;
                if (state.count == length) {

                    if (state.resultEvent != null) {
                        if (expiredResultEvent != null) {
                            state.expiredEventChunk.clear();
                            outputStreamEventChunk.add(expiredResultEvent);
                            outputStreamEventChunk.add(state.resetEvent);

                        }
                        outputStreamEventChunk.add(state.resultEvent);
                        expiredResultEvent = streamEventCloner.copyStreamEvent(state.resultEvent);
                        expiredResultEvent.setTimestamp(currentTime);
                        expiredResultEvent.setType(StreamEvent.Type.EXPIRED);
                        state.expiredEventChunk.add(expiredResultEvent);
                        state.resetEvent = streamEventCloner.copyStreamEvent(state.resultEvent);
                        state.resetEvent.setType(StateEvent.Type.RESET);
                    }
                    state.count = 0;
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }
                }

            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {
        //do nothing
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
        //do nothing
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                                         StreamEventCloner streamEventCloner, ExtensionState state) {
        return ((Operator) compiledCondition).find(matchingEvent, state.expiredEventChunk, streamEventCloner);
    }

    @Override
    public synchronized CompiledCondition compileCondition(Expression condition,
                                                           MatchingMetaInfoHolder matchingMetaInfoHolder,
                                                           List<VariableExpressionExecutor> variableExpressionExecutors,
                                                           Map<String, Table> tableMap, ExtensionState state,
                                                           SiddhiQueryContext siddhiQueryContext) {
        if (state.expiredEventChunk == null) {
            state.expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
        }
        return OperatorParser.constructOperator(state.expiredEventChunk, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    static class ExtensionState extends State {
        private static final String RESULT_EVENT = "resultEvent";
        private static final String EXPIRED_EVENTS = "expiredEvents";
        private static final String COUNT = "count";
        private static final String RESET_EVENT = "resetEvent";

        private int count = 0;
        private StreamEvent resultEvent;
        private StreamEvent resetEvent;
        private ComplexEventChunk<StreamEvent> expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);

        private ExtensionState(StreamEvent resultEvent, StreamEvent resetEvent) {
            this.resultEvent = resultEvent;
            this.resetEvent = resetEvent;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            return expiredEventChunk != null ?
                    new HashMap<String, Object>() {
                        {
                            put(RESULT_EVENT, resultEvent);
                            put(EXPIRED_EVENTS, expiredEventChunk);
                            put(COUNT, Integer.valueOf(count));
                            put(RESET_EVENT, resetEvent);
                        }
                    } :
                    new HashMap<String, Object>() {
                        {
                            put(RESULT_EVENT, resultEvent);
                            put(COUNT, Integer.valueOf(count));
                            put(RESET_EVENT, resetEvent);
                        }
                    };
        }

        @SuppressWarnings("unchecked")
        @Override
        public void restore(Map<String, Object> state) {
            resultEvent = (StreamEvent) state.get(RESULT_EVENT);
            this.count = ((Integer) state.get(COUNT)).intValue();
            resetEvent = (StreamEvent) state.get(RESET_EVENT);
            if (state.size() > 3) {
                expiredEventChunk = (ComplexEventChunk<StreamEvent>) state.get(EXPIRED_EVENTS);
            }
        }
    }
}
