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
import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Abstract class which gives the event which holds minimum or maximum value corresponding to
 * given attribute in a Length window.
 */
public abstract class MaxByMinByLengthWindowProcessor
        extends BatchingFindableWindowProcessor<MaxByMinByLengthWindowProcessor.ExtensionState> {
    String minByMaxByExecutorType;
    private ExpressionExecutor minByMaxByExecutorAttribute;
    private MaxByMinByExecutor maxByMinByExecutor;
    private int length;
    private ComplexEventChunk<StreamEvent> internalWindowChunk = null;
    private ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
    private SiddhiQueryContext siddhiQueryContext;
    private StreamEvent outputStreamEvent;
    private List<StreamEvent> events = new ArrayList<StreamEvent>();

    public void setOutputStreamEvent(StreamEvent outputSreamEvent) {
        synchronized (this) {
            this.outputStreamEvent = outputSreamEvent;
        }
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
        this.internalWindowChunk = new ComplexEventChunk<StreamEvent>(false);
        maxByMinByExecutor = new MaxByMinByExecutor();
        //this.events=new ArrayList<StreamEvent>();
        maxByMinByExecutor.setMinByMaxByExecutorType(minByMaxByExecutorType);

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
        minByMaxByExecutorAttribute = this.attributeExpressionExecutors[0];
        length = (Integer) (((ConstantExpressionExecutor) this.attributeExpressionExecutors[1]).getValue());

        return () -> new ExtensionState(null);
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk the stream event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ExtensionState state) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner
                        .copyStreamEvent(streamEvent); //this cloned event used to
                // insert the event into treemap

                if (state.count != 0) {
                    outputStreamEventChunk.clear();
                    internalWindowChunk.clear();
                }

                //get the parameter value for every events
                Object parameterValue = getParameterValue(minByMaxByExecutorAttribute, streamEvent);

                //insert the cloned event into treemap
                maxByMinByExecutor.insert(clonedStreamEvent, parameterValue);

                if (state.count < length) {
                    state.count++;

                    //get the output event
                    setOutputStreamEvent(maxByMinByExecutor.getResult(maxByMinByExecutor.getMinByMaxByExecutorType()));

                    if (state.toBeExpiredEvent != null) {
                        if (outputStreamEvent != state.toBeExpiredEvent) {
                            state.toBeExpiredEvent.setTimestamp(currentTime);
                            state.toBeExpiredEvent.setType(StateEvent.Type.EXPIRED);
                            outputStreamEventChunk.add(state.toBeExpiredEvent);
                        }
                    }

                    outputStreamEventChunk.add(outputStreamEvent);
                    //add the event which is to be expired
                    internalWindowChunk.add(streamEventCloner.copyStreamEvent(outputStreamEvent));
                    state.toBeExpiredEvent = outputStreamEvent;
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }

                    events.add(clonedStreamEvent);
                } else {
                    StreamEvent firstEvent = events.get(0);
                    if (firstEvent != null) {
                        firstEvent.setTimestamp(currentTime);

                        //remove the expired event from treemap
                        Object expiredEventParameterValue = getParameterValue(minByMaxByExecutorAttribute, firstEvent);

                        maxByMinByExecutor.getSortedEventMap().remove(expiredEventParameterValue);
                        events.remove(0);

                        //get the output event
                        setOutputStreamEvent(
                                maxByMinByExecutor.getResult(maxByMinByExecutor.getMinByMaxByExecutorType()));
                        if (state.toBeExpiredEvent != null) {
                            if (outputStreamEvent != state.toBeExpiredEvent) {
                                state.toBeExpiredEvent.setTimestamp(currentTime);
                                state.toBeExpiredEvent.setType(StateEvent.Type.EXPIRED);
                                outputStreamEventChunk.add(state.toBeExpiredEvent);
                            }

                        }
                        outputStreamEventChunk.add(outputStreamEvent);
                        internalWindowChunk.add(streamEventCloner.copyStreamEvent(outputStreamEvent));
                        state.toBeExpiredEvent = outputStreamEvent;

                        if (outputStreamEventChunk.getFirst() != null) {
                            streamEventChunks.add(outputStreamEventChunk);
                        }
                        events.add(clonedStreamEvent);

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
        //Do nothing
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
        //Do nothing
    }

    /**
     * To find the attribute value of given attribute for each event .
     *
     * @param functionParameter name of the parameter of the event data
     * @param streamEvent       event  at processor
     * @return the attributeValue
     */

    public Object getParameterValue(ExpressionExecutor functionParameter, StreamEvent streamEvent) {
        Object attributeValue;
        attributeValue = functionParameter.execute(streamEvent);
        return attributeValue;
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, ExtensionState state) {
        return ((Operator) compiledCondition).find(matchingEvent, internalWindowChunk, streamEventCloner);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, ExtensionState state,
                                              SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(this.internalWindowChunk, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
    }

    static class ExtensionState extends State {

        private int count = 0;
        StreamEvent toBeExpiredEvent;

        private ExtensionState(StreamEvent toBeExpiredEvent) {
            this.toBeExpiredEvent = toBeExpiredEvent;
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
                        put("toBeExpiredEvent", toBeExpiredEvent);
                        put("count", count);
                    }
                };
            }
        }

        @Override
        public void restore(Map<String, Object> state) {
            synchronized (this) {
                toBeExpiredEvent = (StreamEvent) state.get("toBeExpiredEvent");
                count = (Integer) state.get("count");
            }
        }
    }
}
