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
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;

/**
 * Abstract class which gives the min/max event in a Time Batch Window
 * according to a given attribute
 */

public abstract class MaxByMinByTimeBatchWindowProcessor extends WindowProcessor
        implements SchedulingProcessor, FindableProcessor {
    protected String maxByMinByType;
    protected String windowType;
    private long timeInMilliSeconds;
    private long nextEmitTime = -1;
    private StreamEvent resetEvent = null;
    private Scheduler scheduler;
    private ExecutionPlanContext executionPlanContext;
    private boolean isStartTimeEnabled = false;
    private long startTime = 0;
    private ExpressionExecutor sortByAttribute;
    private StreamEvent currentEvent = null;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override protected void init(ExpressionExecutor[] attributeExpressionExecutors,
            ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        if (attributeExpressionExecutors.length == 2) {
            Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
            sortByAttribute = attributeExpressionExecutors[0];
            if (!((attributeType == Attribute.Type.DOUBLE) || (attributeType == Attribute.Type.INT) || (attributeType
                    == Attribute.Type.FLOAT) || (attributeType == Attribute.Type.LONG) || (attributeType
                    == Attribute.Type.STRING))) {
                throw new ExecutionPlanValidationException(
                        "Invalid parameter type found for the first argument of " + windowType + " " + "required "
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
                    throw new ExecutionPlanValidationException(
                            "Time Batch window's parameter attribute should be either int or long, but found "
                                    + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException(
                        "Time Batch window should have constant parameter attribute but found a dynamic attribute "
                                + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else if (attributeExpressionExecutors.length == 3) {
            Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
            sortByAttribute = attributeExpressionExecutors[0];
            if (!((attributeType == Attribute.Type.DOUBLE) || (attributeType == Attribute.Type.INT) || (attributeType
                    == Attribute.Type.FLOAT) || (attributeType == Attribute.Type.LONG) || (attributeType
                    == Attribute.Type.STRING))) {
                throw new ExecutionPlanValidationException(
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
                    throw new ExecutionPlanValidationException(
                            "Time window's parameter attribute should be either int or long, but found "
                                    + attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException(
                        "Time window should have constant parameter attribute but found a dynamic attribute "
                                + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            // start time
            isStartTimeEnabled = true;
            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                startTime = Integer.parseInt(
                        String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
            } else {
                startTime = Long.parseLong(
                        String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
            }
        } else {
            throw new ExecutionPlanValidationException(
                    windowType + " should only have two or three parameters. but found "
                            + attributeExpressionExecutors.length + " input attributes");
        }
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner) {
        synchronized (this) {
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            if (nextEmitTime == -1) {
                if (isStartTimeEnabled) {
                    nextEmitTime = getNextEmitTime(currentTime);
                } else {
                    nextEmitTime = executionPlanContext.getTimestampGenerator().currentTime() + timeInMilliSeconds;
                }
                scheduler.notifyAt(nextEmitTime);
            }

            boolean sendEvents;
            if (currentTime >= nextEmitTime) {
                nextEmitTime += timeInMilliSeconds;
                scheduler.notifyAt(nextEmitTime);
                sendEvents = true;
            } else {
                sendEvents = false;
            }

            //for each event, set the min/max event as current event
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                if (maxByMinByType.equals(MaxByMinByConstants.MIN_BY)) {
                    currentEvent = MaxByMinByExecutor
                            .getMinEventBatchProcessor(clonedStreamEvent, currentEvent, sortByAttribute);
                } else if (maxByMinByType.equals(MaxByMinByConstants.MAX_BY)) {
                    currentEvent = MaxByMinByExecutor
                            .getMaxEventBatchProcessor(clonedStreamEvent, currentEvent, sortByAttribute);
                }
            }
            streamEventChunk.clear();
            if (sendEvents) {
                if (expiredEventChunk.getFirst() != null) {
                    while (expiredEventChunk.hasNext()) {
                        StreamEvent expiredEvent = expiredEventChunk.next();
                        expiredEvent.setTimestamp(currentTime);
                    }
                    if (outputExpectsExpiredEvents) {
                        streamEventChunk.add(expiredEventChunk.getFirst());
                    }
                    resetEvent = streamEventCloner.copyStreamEvent(expiredEventChunk.getFirst());
                    resetEvent.setType(ComplexEvent.Type.RESET);
                    // add reset event before the current events
                    if (resetEvent != null) {
                        streamEventChunk.add(resetEvent);
                    }
                    resetEvent = null;
                }

                if (expiredEventChunk != null) {
                    expiredEventChunk.clear();
                }
                if (currentEvent != null) {
                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                    expiredEventChunk.add(toExpireEvent);
                    streamEventChunk.add(currentEvent);
                }
                currentEvent = null;

            }
        }
        if (streamEventChunk.getFirst() != null) {
            streamEventChunk.setBatch(true);
            nextProcessor.process(streamEventChunk);
            streamEventChunk.setBatch(false);
        }
    }

    private long getNextEmitTime(long currentTime) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - startTime) % timeInMilliSeconds;
        long emitTime = currentTime + (timeInMilliSeconds - elapsedTimeSinceLastEmit);
        return emitTime;
    }

    /**
     * The getScheduler method of the TimeWindowProcessor.
     * Since scheduler is a private variable, setter method is for public access.
     */
    @Override public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * The setScheduler method of the TimeWindowProcessor.
     * Since scheduler is a private variable, setter method is for public access.
     *
     * @param scheduler the value of scheduler.
     */
    @Override public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override public void start() {
        //Do nothing
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override public void stop() {
        //Do nothing
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override public Object[] currentState() {
        if (expiredEventChunk != null) {
            return new Object[] { currentEvent, expiredEventChunk.getFirst(), resetEvent };
        } else {
            return new Object[] { currentEvent, resetEvent };
        }
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on
     *              the same order provided by currentState().
     */
    @Override public void restoreState(Object[] state) {
        if (state.length > 2) {
            currentEvent = (StreamEvent) state[0];
            expiredEventChunk.clear();
            expiredEventChunk.add((StreamEvent) state[1]);
            resetEvent = (StreamEvent) state[2];
        } else {
            currentEvent = (StreamEvent) state[0];
            resetEvent = (StreamEvent) state[1];
        }
    }

    /**
     * To find events from the processor event pool, that the matches the matchingEvent based on finder logic.
     *
     * @param matchingEvent the event to be matched with the events at the processor
     * @param finder        the execution element responsible for finding the corresponding events that matches
     *                      the matchingEvent based on pool of events at Processor
     * @return the matched events
     */
    @Override public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, expiredEventChunk, streamEventCloner);
    }

    /**
     * To construct a finder having the capability of finding events at the processor that corresponds to the incoming
     * matchingEvent and the given matching expression logic.
     *
     * @param expression                  the matching expression
     * @param matchingMetaStateHolder     the meta structure of the incoming matchingEvent
     * @param executionPlanContext        current execution plan context
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param eventTableMap               map of event tables
     * @return finder having the capability of finding events at the processor against the expression and incoming
     * matchingEvent
     */
    @Override public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
            ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors,
            Map<String, EventTable> eventTableMap) {
        if (expiredEventChunk == null) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        }
        return OperatorParser
                .constructOperator(expiredEventChunk, expression, matchingMetaStateHolder, executionPlanContext,
                        variableExpressionExecutors, eventTableMap);
    }

}
