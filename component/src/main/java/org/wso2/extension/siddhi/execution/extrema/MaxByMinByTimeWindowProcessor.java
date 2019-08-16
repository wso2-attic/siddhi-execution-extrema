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
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.query.processor.stream.window.BatchingFindableWindowProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.Scheduler;
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Abstract class which gives the min/max event in a Time Window
 * according to given attribute as events arrive and expire
 */

public abstract class MaxByMinByTimeWindowProcessor
        extends BatchingFindableWindowProcessor<MaxByMinByTimeWindowProcessor.ExtensionState>
        implements SchedulingProcessor {

    String maxByMinByType;
    String windowType;
    private long timeInMilliSeconds;
    private Scheduler scheduler;
    private SiddhiQueryContext siddhiQueryContext;
    private volatile long lastTimestamp = Long.MIN_VALUE;
    private ExpressionExecutor sortByAttribute;
    private StreamEvent currentEvent;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;

    /**
     * The getScheduler method of the TimeWindowProcessor.
     * Since scheduler is a private variable, setter method is for public access.
     */
    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * The setScheduler method of the TimeWindowProcessor.
     * Since scheduler is a private variable, setter method is for public access.
     *
     * @param scheduler the value of scheduler.
     */
    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
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
        this.expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        MaxByMinByExecutor minByMaxByExecutor = new MaxByMinByExecutor();
        if (attributeExpressionExecutors.length == 2) {
            Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
            sortByAttribute = attributeExpressionExecutors[0];
            if (!((attributeType == Attribute.Type.DOUBLE) || (attributeType == Attribute.Type.INT) || (attributeType
                    == Attribute.Type.FLOAT) || (attributeType == Attribute.Type.LONG) || (attributeType
                    == Attribute.Type.STRING))) {
                throw new SiddhiAppValidationException(
                        "Invalid parameter type found for the first argument of " + windowType + " required "
                                + Attribute.Type.INT + " or " + Attribute.Type.LONG + " or " + Attribute.Type.FLOAT
                                + " or " + Attribute.Type.DOUBLE + " or " + Attribute.Type.STRING + ", but found "
                                + attributeType.toString());
            }

            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();

                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                } else {
                    throw new SiddhiAppValidationException(
                            "Time parameter should be either int or long, but found " + attributeExpressionExecutors[1]
                                    .getReturnType());
                }
            } else {
                throw new SiddhiAppValidationException(
                        "Time parameter should have constant parameter attribute but found a dynamic attribute "
                                + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Invalid no of arguments passed to " + windowType + ", " + "required 2, but found "
                            + attributeExpressionExecutors.length + " input attributes");
        }
        return () -> new ExtensionState(minByMaxByExecutor);
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
        synchronized (this) {
            StreamEvent streamEvent = null;
            while (streamEventChunk.hasNext()) {
                streamEvent = streamEventChunk.next();
                long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();

                // Iterate through the sortedEventMap and remove the expired events
                Set set = state.minByMaxByExecutor.getSortedEventMap().entrySet();
                Iterator iterator = set.iterator();
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    StreamEvent expiredEvent = (StreamEvent) entry.getValue();
                    long timeDiff = expiredEvent.getTimestamp() - currentTime + timeInMilliSeconds;
                    if (timeDiff <= 0) {
                        iterator.remove();
                    }
                }
                //remove expired events from the expiredEventChunk
                expiredEventChunk.reset();
                while (expiredEventChunk.hasNext()) {
                    StreamEvent toExpiredEvent = expiredEventChunk.next();
                    long timeDiff = toExpiredEvent.getTimestamp() - currentTime + timeInMilliSeconds;
                    if (timeDiff <= 0) {
                        expiredEventChunk.remove();
                        toExpiredEvent.setType(StreamEvent.Type.EXPIRED);
                        toExpiredEvent.setTimestamp(currentTime);
                        streamEventChunk.insertBeforeCurrent(toExpiredEvent);
                    }
                }

                //Add the current event to sortedEventMap
                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    state.minByMaxByExecutor.insert(clonedEvent, sortByAttribute.execute(clonedEvent));
                    if (lastTimestamp < clonedEvent.getTimestamp()) {
                        scheduler.notifyAt(clonedEvent.getTimestamp() + timeInMilliSeconds);
                        lastTimestamp = clonedEvent.getTimestamp();
                    }
                }
                streamEventChunk.remove();
            }
            expiredEventChunk.reset();
            //retrieve the min/max event and add to streamEventChunk
            if (streamEvent != null && streamEvent.getType() == StreamEvent.Type.CURRENT) {
                StreamEvent tempEvent;
                if (maxByMinByType.equals(MaxByMinByConstants.MIN_BY)) {
                    tempEvent = state.minByMaxByExecutor.getResult(MaxByMinByConstants.MIN_BY);
                } else {
                    tempEvent = state.minByMaxByExecutor.getResult(MaxByMinByConstants.MAX_BY);
                }
                if (tempEvent != currentEvent) {
                    StreamEvent event = streamEventCloner.copyStreamEvent(tempEvent);
                    expiredEventChunk.add(event);
                    currentEvent = tempEvent;
                    streamEventChunk.add(currentEvent);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, ExtensionState state) {
        return ((Operator) compiledCondition).find(matchingEvent, expiredEventChunk, streamEventCloner);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition,
                                              MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, ExtensionState state,
                                              SiddhiQueryContext siddhiQueryContext) {
        return OperatorParser.constructOperator(expiredEventChunk, condition, matchingMetaInfoHolder,
                variableExpressionExecutors, tableMap, siddhiQueryContext);
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

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.SLIDE;
    }

    static class ExtensionState extends State {

        private MaxByMinByExecutor minByMaxByExecutor;

        private ExtensionState(MaxByMinByExecutor minByMaxByExecutor) {
            this.minByMaxByExecutor = minByMaxByExecutor;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            return new HashMap<String, Object>() {
                {
                    put("sortedMap", minByMaxByExecutor.getSortedEventMap());
                }
            };
        }

        @SuppressWarnings("unchecked")
        @Override
        public void restore(Map<String, Object> state) {
            minByMaxByExecutor.setSortedEventMap((TreeMap) state.get("sortedMap"));
        }
    }
}

