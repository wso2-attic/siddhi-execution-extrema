/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
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
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OperatorParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;
import org.wso2.extension.siddhi.execution.extrema.util.AbstractTopKBottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.Constants;
import org.wso2.extension.siddhi.execution.extrema.util.Counter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sample Query (For topKLengthBatch implementation):
 * from inputStream#extrema:topKTimeBatch(attribute1, 1 sec, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Sample Query (For bottomKLengthBatch implementation):
 * from inputStream#extrema:bottomKTimeBatch(attribute1, 1 sec, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Description:
 * In the example query given, 1 sec is the duration of the window, 3 is the k-value and attribute1
 * is the attribute of which the frequency is counted. The frequencies of the values received for
 * the attribute given will be counted by this and the topK/bottomK values will be emitted per batch.
 * Events will not emit if there is no change from the last send topK/bottomK results
 */
public abstract class AbstractKTimeBatchStreamProcessorExtension
        extends StreamProcessor<AbstractKTimeBatchStreamProcessorExtension.ExtensionState>
        implements SchedulingProcessor, FindableProcessor {

    private Scheduler scheduler;
    private VariableExpressionExecutor attrVariableExpressionExecutor;

    private long nextEmitTime = -1;
    private boolean isStartTimeEnabled = false;
    private Object[] lastOutputData;
    protected List<Attribute> attributeList = new ArrayList<>();

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           ExtensionState state) {
        ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
        synchronized (this) {
            if (nextEmitTime == -1) {   // In the first time process method is called
                long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
                if (isStartTimeEnabled) {
                    long elapsedTimeSinceLastEmit = (currentTime - state.startTime) % state.windowTime;
                    nextEmitTime = currentTime + (state.windowTime - elapsedTimeSinceLastEmit);
                } else {
                    nextEmitTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime()
                            + state.windowTime;
                }
                state.topKBottomKFinder = createNewTopKBottomKFinder();
                scheduler.notifyAt(nextEmitTime);
            }

            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            boolean sendEvents = false;
            if (currentTime >= nextEmitTime) {
                nextEmitTime += state.windowTime;
                scheduler.notifyAt(nextEmitTime);
                sendEvents = true;
            }

            if (currentTime >= state.startTime) {
                while (streamEventChunk.hasNext()) {
                    StreamEvent streamEvent = streamEventChunk.next();

                    // New current event tasks
                    if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                        state.lastStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        state.topKBottomKFinder.offer(attrVariableExpressionExecutor.execute(state.lastStreamEvent));
                    }
                }
            }

            // End of window tasks
            if (sendEvents) {
                if (state.expiredEventChunk.getFirst() != null) {
                    // Adding the expired events
                    if (state.outputExpectsExpiredEvents) {
                        state.expiredEventChunk.getFirst().setTimestamp(currentTime);
                        outputStreamEventChunk.add(state.expiredEventChunk.getFirst());
                        state.expiredEventChunk.clear();
                    }

                    // Adding the reset event
                    StreamEvent resetEvent = streamEventCloner.copyStreamEvent(state.lastStreamEvent);
                    resetEvent.setType(ComplexEvent.Type.RESET);
                    outputStreamEventChunk.add(resetEvent);
                }

                // Adding the last event with the topK frequencies for the window
                if (state.lastStreamEvent != null) {
                    Object[] outputStreamEventData = new Object[2 * state.querySize];
                    List<Counter<Object>> topKCounters = state.topKBottomKFinder.get(state.querySize);
                    boolean isSameAsLastEmission = true;
                    int i = 0;
                    while (i < topKCounters.size()) {
                        Counter<Object> topKCounter = topKCounters.get(i);
                        outputStreamEventData[2 * i] = topKCounter.getItem();
                        outputStreamEventData[2 * i + 1] = topKCounter.getCount();
                        if (lastOutputData == null ||
                                lastOutputData[2 * i] != outputStreamEventData[2 * i] ||
                                lastOutputData[2 * i + 1] != outputStreamEventData[2 * i + 1]) {
                            isSameAsLastEmission = false;
                        }
                        i++;
                    }
                    if (!isSameAsLastEmission) {
                        lastOutputData = outputStreamEventData;
                        complexEventPopulater.populateComplexEvent(state.lastStreamEvent, outputStreamEventData);
                        outputStreamEventChunk.add(state.lastStreamEvent);

                        // Setting the event to be expired in the next window
                        StreamEvent expiredStreamEvent = streamEventCloner.copyStreamEvent(state.lastStreamEvent);
                        expiredStreamEvent.setType(ComplexEvent.Type.EXPIRED);
                        state.expiredEventChunk.add(expiredStreamEvent);
                    }
                }

                // Resetting window
                state.topKBottomKFinder = createNewTopKBottomKFinder();
            }
        }

        if (outputStreamEventChunk.getFirst() != null) {
            streamEventChunk.setBatch(true);
            nextProcessor.process(outputStreamEventChunk);
            streamEventChunk.setBatch(false);
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
        int querySize;
        long windowTime;
        long startTime = 0L;
        ComplexEventChunk<StreamEvent> expiredEventChunk;
        if (attributeExpressionExecutors.length == 3 ||
                attributeExpressionExecutors.length == 4) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
        } else {
            throw new SiddhiAppValidationException(
                    "3 arguments (4 arguments if start time is also specified) should be " +
                            "passed to " + getExtensionNamePrefix() + "KTimeBatchStreamProcessor, but found " +
                            attributeExpressionExecutors.length
            );
        }

        // Checking the topK/bottomK attribute
        if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            attrVariableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        } else {
            throw new SiddhiAppValidationException("Attribute for ordering in " +
                    getExtensionNamePrefix() +
                    "KTimeBatchStreamProcessor should be a variable, but found a constant attribute " +
                    attributeExpressionExecutors[1].getClass().getCanonicalName()
            );
        }

        // Checking the window time parameter
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[1].getReturnType();
            if (attributeType == Attribute.Type.LONG) {
                windowTime = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else if (attributeType == Attribute.Type.INT) {
                windowTime = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new SiddhiAppValidationException(
                        "Window time parameter for " + getExtensionNamePrefix() +
                                "KTimeBatchStreamProcessor should be INT or LONG, but found " + attributeType
                );
            }
            if (windowTime <= 0) {
                throw new SiddhiAppValidationException(
                        "Window time parameter for " + getExtensionNamePrefix() +
                                "KTimeBatchStreamProcessor should be greater than 0, but found " + attributeType
                );
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Window time parameter for " + getExtensionNamePrefix() +
                            "KTimeBatchStreamProcessor should be a constant, but found a dynamic attribute " +
                            attributeExpressionExecutors[1].getClass().getCanonicalName()
            );
        }

        // Checking the query size parameter
        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[2].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                querySize = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
                if (querySize <= 0) {
                    throw new SiddhiAppValidationException(
                            "Query size parameter for " + getExtensionNamePrefix() +
                                    "KLengthBatchStreamProcessor should be greater than 0, but found " + attributeType
                    );
                }
            } else {
                throw new SiddhiAppValidationException(
                        "Query size parameter for " + getExtensionNamePrefix() +
                                "KTimeBatchStreamProcessor should be INT, but found " + attributeType
                );
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Query size parameter for " + getExtensionNamePrefix() +
                            "KTimeBatchStreamProcessor should be a constant, but found a dynamic attribute " +
                            attributeExpressionExecutors[2].getClass().getCanonicalName()
            );
        }

        if (attributeExpressionExecutors.length == 4) {
            if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                Attribute.Type attributeType = attributeExpressionExecutors[3].getReturnType();
                if (attributeType == Attribute.Type.INT) {
                    isStartTimeEnabled = true;
                    startTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime() +
                            (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue();
                    if (startTime < 0) {
                        throw new SiddhiAppValidationException(
                                "Start time parameter for " + getExtensionNamePrefix() +
                                        "KTimeBatchStreamProcessor should be greater than or equal to 0," +
                                        "but found " + attributeType
                        );
                    }
                } else {
                    throw new SiddhiAppValidationException(
                            "Start time parameter for " + getExtensionNamePrefix() +
                                    "KTimeBatchStreamProcessor should be INT, but found " + attributeType
                    );
                }
            }
        }

        // Generating the list of additional attributes added to the events sent out
        List<Attribute> newAttributes = new ArrayList<Attribute>();
        for (int i = 0; i < querySize; i++) {
            newAttributes.add(new Attribute(
                    getExtensionNamePrefix() + (i + 1) + Constants.TOP_K_BOTTOM_K_ELEMENT,
                    attrVariableExpressionExecutor.getReturnType())
            );
            newAttributes.add(new Attribute(
                    getExtensionNamePrefix() + (i + 1) + Constants.TOP_K_BOTTOM_K_FREQUENCY,
                    Attribute.Type.LONG)
            );
        }
        this.attributeList = newAttributes;
        long finalStartTime = startTime;
        return () -> new ExtensionState(querySize, windowTime, finalStartTime, outputExpectsExpiredEvents,
        null, null, expiredEventChunk);
    }

    @Override
    public void start() {
        // Do Nothing
    }

    @Override
    public void stop() {
        // Do Nothing
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        ExtensionState state = stateHolder.getState();
        try {
            return find(matchingEvent, compiledCondition, streamEventClonerHolder.getStreamEventCloner(),
                    state);
        } finally {
            stateHolder.returnState(state);
        }

    }

    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition,
                            StreamEventCloner streamEventCloner, ExtensionState state) {
        return ((Operator) compiledCondition).find(matchingEvent, state.expiredEventChunk, streamEventCloner);
    }

    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        ExtensionState state = stateHolder.getState();
        try {
            return compileCondition(condition, matchingMetaInfoHolder, variableExpressionExecutors, tableMap,
                    state, siddhiQueryContext);
        } finally {
            stateHolder.returnState(state);
        }
    }

    public CompiledCondition compileCondition(Expression condition,
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

    /**
     * Create and return either a TopKFinder or a BottomKFinder
     * Should be implemented by the child classes which will determine whether it is top K or bottom K
     *
     * @return TopKFinder or BottomKFinder
     */
    protected abstract AbstractTopKBottomKFinder<Object> createNewTopKBottomKFinder();

    /**
     * Return the name prefix that should be used in
     * the returning extra parameters and in the SiddhiAppValidationException that might get thrown
     * Should be either "Top" or "Bottom" to indicate whether it is top K or bottom K
     *
     * @return Name prefix. Should be either "Top" or "Bottom"
     */
    protected abstract String getExtensionNamePrefix();

    static class ExtensionState extends State {
        private static final String TOP_K_BOTTOM_K_FINDER = "topKBottomKFinder";
        private static final String WINDOW_TIME = "windowTime";
        private static final String QUERY_SIZE = "querySize";
        private static final String START_TIME = "startTime";
        private static final String LAST_STREAM_EVENT = "lastStreamEvent";
        private static final String EXPIRED_EVENT_CHUNK = "expiredEventChunk";

        private int querySize;          // The K value
        private long windowTime;
        private long startTime = 0L;
        private boolean outputExpectsExpiredEvents;
        private AbstractTopKBottomKFinder<Object> topKBottomKFinder;

        // Event used for adding the topK/bottomK items and frequencies added to it
        private StreamEvent lastStreamEvent;
        private ComplexEventChunk<StreamEvent> expiredEventChunk;

        private ExtensionState(int querySize, long windowTime, long startTime, boolean outputExpectsExpiredEvents,
                               StreamEvent lastStreamEvent, AbstractTopKBottomKFinder<Object> topKBottomKFinder,
                               ComplexEventChunk<StreamEvent> expiredEventChunk) {
            this.querySize = querySize;
            this.windowTime = windowTime;
            this.startTime = startTime;
            this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
            this.lastStreamEvent = lastStreamEvent;
            this.topKBottomKFinder = topKBottomKFinder;
            this.expiredEventChunk = expiredEventChunk;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            synchronized (this) {
                if (outputExpectsExpiredEvents) {
                    return new HashMap<String, Object>() {
                        {
                            put(TOP_K_BOTTOM_K_FINDER, topKBottomKFinder);
                            put(WINDOW_TIME, windowTime);
                            put(QUERY_SIZE, querySize);
                            put(START_TIME, startTime);
                            put(LAST_STREAM_EVENT, lastStreamEvent);
                            put(EXPIRED_EVENT_CHUNK, expiredEventChunk);
                        }
                    };
                } else {
                    return new HashMap<String, Object>() {
                        {
                            put(TOP_K_BOTTOM_K_FINDER, topKBottomKFinder);
                            put(WINDOW_TIME, windowTime);
                            put(QUERY_SIZE, querySize);
                            put(START_TIME, startTime);
                            put(LAST_STREAM_EVENT, lastStreamEvent);
                        }
                    };
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void restore(Map<String, Object> state) {
            synchronized (this) {
                topKBottomKFinder = (AbstractTopKBottomKFinder<Object>) state.get(TOP_K_BOTTOM_K_FINDER);
                windowTime = (Long) state.get(WINDOW_TIME);
                querySize = (Integer) state.get(QUERY_SIZE);
                startTime = (Long) state.get(START_TIME);

                lastStreamEvent = (StreamEvent) state.get(LAST_STREAM_EVENT);
                if (state.size() == 6) {
                    expiredEventChunk = (ComplexEventChunk<StreamEvent>) state.get(EXPIRED_EVENT_CHUNK);
                }
            }
        }
    }
}
