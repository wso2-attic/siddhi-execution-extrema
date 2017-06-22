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

import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByConstants;
import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByExecutor;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract class which gives the min/max event in a LengthBatch window
 */

public abstract class MaxByMinByLengthBatchWindowProcessor extends WindowProcessor implements FindableProcessor {

    private static final String RESULT_EVENT = "resultEvent";
    private static final String EXPIRED_EVENTS = "expiredEvents";
    private static final String COUNT = "count";
    private static final String RESET_EVENT = "resetEvent";
    protected String minByMaxByExecutorType;
    private int length;
    private int count = 0;
    private ExpressionExecutor minByMaxByExecutorAttribute;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private SiddhiAppContext siddhiAppContext;
    private VariableExpressionExecutor[] variableExpressionExecutors;
    private MaxByMinByExecutor minByMaxByExecutor;
    private StreamEvent oldEvent;
    private StreamEvent resultEvent;
    private StreamEvent expiredResultEvent;
    private StreamEvent resetEvent;

    public MaxByMinByLengthBatchWindowProcessor() {

    }

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param siddhiAppContext             the context of the execution plan
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        boolean outputExpectsExpiredEvents, SiddhiAppContext siddhiAppContext) {

        this.siddhiAppContext = siddhiAppContext;
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
                           StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent currentEvent = streamEventCloner.copyStreamEvent(streamEvent);

                if (count == 0) {
                    outputStreamEventChunk.clear();
                    oldEvent = null;
                    //clonedResultEvent=resultEvent;
                }

                //Get the event which hold the minimum or maximum event

                if (minByMaxByExecutorType.equals(MaxByMinByConstants.MAX_BY)) {
                    resultEvent = MaxByMinByExecutor
                            .getMaxEventBatchProcessor(currentEvent, oldEvent, minByMaxByExecutorAttribute);
                    oldEvent = resultEvent;
                } else if (minByMaxByExecutorType.equals(MaxByMinByConstants.MIN_BY)) {
                    resultEvent = MaxByMinByExecutor
                            .getMinEventBatchProcessor(currentEvent, oldEvent, minByMaxByExecutorAttribute);
                    oldEvent = resultEvent;
                }

                count++;
                if (count == length) {

                    if (resultEvent != null) {
                        if (expiredResultEvent != null) {
                            expiredEventChunk.clear();
                            outputStreamEventChunk.add(expiredResultEvent);
                            outputStreamEventChunk.add(resetEvent);

                        }
                        outputStreamEventChunk.add(resultEvent);
                        expiredResultEvent = streamEventCloner.copyStreamEvent(resultEvent);
                        expiredResultEvent.setTimestamp(currentTime);
                        expiredResultEvent.setType(StreamEvent.Type.EXPIRED);
                        expiredEventChunk.add(expiredResultEvent);
                        resetEvent = streamEventCloner.copyStreamEvent(resultEvent);
                        resetEvent.setType(StateEvent.Type.RESET);
                    }
                    count = 0;
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

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Map<String, Object> currentState() {
        return this.expiredEventChunk != null ?
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

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as a hashmap.
     */
    @Override
    public synchronized void restoreState(Map<String, Object> state) {
        resultEvent = (StreamEvent) state.get(RESULT_EVENT);
        this.count = ((Integer) state.get(COUNT)).intValue();
        this.resetEvent = (StreamEvent) state.get(RESET_EVENT);
        if (state.size() > 3) {
            expiredEventChunk = (ComplexEventChunk<StreamEvent>) state.get(EXPIRED_EVENTS);
        }
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        return ((Operator) compiledCondition).find(matchingEvent, expiredEventChunk, streamEventCloner);
    }

    @Override
    public synchronized CompiledCondition compileCondition(Expression expression,
                                                           MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              SiddhiAppContext siddhiAppContext,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, String queryName) {
        if (expiredEventChunk == null) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
        }
        return OperatorParser.constructOperator(expiredEventChunk, expression, matchingMetaInfoHolder,
                siddhiAppContext, variableExpressionExecutors, tableMap, this.queryName);
    }
}
