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
import io.siddhi.core.query.processor.stream.StreamProcessor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.table.Table;
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
 * from inputStream#extrema:topKLengthBatch(attribute1, 6, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Sample Query (For bottomKLengthBatch implementation):
 * from inputStream#extrema:bottomKLengthBatch(attribute1, 6, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Description:
 * In the example query given, 6 is the length of the window, 3 is the k-value and
 * attribute1 is the attribute of which the frequency is counted.
 * The frequencies of the values received for the attribute given will be counted by this
 * and the topK/bottomK values will be emitted per batch.
 * Events will not emit if there is no change from the last send topK/bottomK results
 */
public abstract class AbstractKLengthBatchStreamProcessorExtension
        extends StreamProcessor<AbstractKLengthBatchStreamProcessorExtension.ExtensionState>
        implements FindableProcessor {

    private VariableExpressionExecutor attrVariableExpressionExecutor;
    private Object[] lastOutputData;
    protected List<Attribute> attributeList = new ArrayList<>();

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                                    StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                                    ExtensionState state) {
        ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
        synchronized (this) {
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                // Current event arrival tasks
                if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                    state.topKBottomKFinder.offer(attrVariableExpressionExecutor.execute(streamEvent));
                    state.count++;
                }

                // Window end tasks
                if (state.count == state.windowLength) {
                    StreamEvent lastStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                    if (state.expiredEventChunk.getFirst() != null) {
                        // Adding expired events
                        if (state.outputExpectsExpiredEvents) {
                            state.expiredEventChunk.getFirst().setTimestamp(currentTime);
                            outputStreamEventChunk.add(state.expiredEventChunk.getFirst());
                            state.expiredEventChunk.clear();
                        }

                        // Adding the reset event
                        StreamEvent resetEvent = streamEventCloner.copyStreamEvent(lastStreamEvent);
                        resetEvent.setType(ComplexEvent.Type.RESET);
                        outputStreamEventChunk.add(resetEvent);
                    }

                    // Generating the list of additional attributes added to the events sent out
                    List<Counter<Object>> topKCounters = state.topKBottomKFinder.get(state.querySize);
                    Object[] outputStreamEventData = new Object[2 * state.querySize];
                    boolean sendEvents = false;
                    int i = 0;
                    while (i < topKCounters.size()) {
                        Counter<Object> topKCounter = topKCounters.get(i);
                        outputStreamEventData[2 * i] = topKCounter.getItem();
                        outputStreamEventData[2 * i + 1] = topKCounter.getCount();
                        if (lastOutputData == null ||
                                lastOutputData[2 * i] != outputStreamEventData[2 * i] ||
                                lastOutputData[2 * i + 1] != outputStreamEventData[2 * i + 1]) {
                            sendEvents = true;
                        }
                        i++;
                    }
                    if (sendEvents) {
                        lastOutputData = outputStreamEventData;
                        complexEventPopulater.populateComplexEvent(lastStreamEvent, outputStreamEventData);
                        outputStreamEventChunk.add(lastStreamEvent);

                        // Setting the event expired in this window
                        StreamEvent expiredStreamEvent = streamEventCloner.copyStreamEvent(lastStreamEvent);
                        expiredStreamEvent.setType(ComplexEvent.Type.EXPIRED);
                        state.expiredEventChunk.add(expiredStreamEvent);
                    }

                    // Resetting window
                    state.topKBottomKFinder = createNewTopKBottomKFinder();
                    state.count = 0;
                }
            }
        }

        if (outputStreamEventChunk.getFirst() != null) {
            nextProcessor.process(outputStreamEventChunk);
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
        int count;
        int windowLength;
        int querySize;
        AbstractTopKBottomKFinder<Object> topKBottomKFinder;
        ComplexEventChunk<StreamEvent> expiredEventChunk;
        if (attributeExpressionExecutors.length == 3) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
            count = 0;
        } else {
            throw new SiddhiAppValidationException(
                    "3 arguments should be passed to " + getExtensionNamePrefix() +
                            "KLengthBatchStreamProcessor, but found " + attributeExpressionExecutors.length
            );
        }

        // Checking the topK/bottomK attribute
        if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            attrVariableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        } else {
            throw new SiddhiAppValidationException(
                    "Attribute for ordering in " + getExtensionNamePrefix() +
                            "KLengthBatchStreamProcessor should be a variable. but found a constant attribute " +
                            attributeExpressionExecutors[1].getClass().getCanonicalName()
            );
        }

        // Checking the window length parameter
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[1].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                windowLength = (Integer) (
                        (ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                if (windowLength <= 0) {
                    throw new SiddhiAppValidationException(
                            "Window length parameter for " + getExtensionNamePrefix() +
                                    "KLengthBatchStreamProcessor should be greater than 0. but found " + attributeType
                    );
                }
                topKBottomKFinder = createNewTopKBottomKFinder();
            } else {
                throw new SiddhiAppValidationException(
                        "Window length parameter for " + getExtensionNamePrefix() +
                                "KLengthBatchStreamProcessor should be INT. but found " + attributeType
                );
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Window length parameter for " + getExtensionNamePrefix() +
                            "KLengthBatchWindowProcessor should be a constant. but found a dynamic attribute " +
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
                                    "KLengthBatchStreamProcessor should be greater than 0. but found " + attributeType
                    );
                }
            } else {
                throw new SiddhiAppValidationException(
                        "Query size parameter for " + getExtensionNamePrefix() +
                                "KLengthBatchWindowProcessor should be INT. but found " + attributeType
                );
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Query size parameter for " + getExtensionNamePrefix() +
                            "KLengthBatchWindowProcessor should be a constant. but found a dynamic attribute " +
                            attributeExpressionExecutors[2].getClass().getCanonicalName());
        }

        // Generating the list of additional attributes added to the events sent out
        List<Attribute> newAttributes = new ArrayList<Attribute>();
        for (int i = 0; i < querySize; i++) {
            newAttributes.add(new Attribute(
                    getExtensionNamePrefix() + (i + 1) + Constants.TOP_K_BOTTOM_K_ELEMENT,
                    attrVariableExpressionExecutor.getReturnType()
            ));
            newAttributes.add(new Attribute(
                    getExtensionNamePrefix() + (i + 1) + Constants.TOP_K_BOTTOM_K_FREQUENCY,
                    Attribute.Type.LONG
            ));
        }
        this.attributeList = newAttributes;
        return () -> new ExtensionState(windowLength, querySize, outputExpectsExpiredEvents, count, expiredEventChunk,
                topKBottomKFinder);
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
     * Return the name prefix that should be used in the returning extra parameters and
     * in the exception that might get thrown
     * Should be either "Top" or "Bottom" to indicate whether it is top K or bottom K
     *
     * @return Name prefix. Should be either "Top" or "Bottom"
     */
    protected abstract String getExtensionNamePrefix();

    static class ExtensionState extends State {
        private static final String TOP_K_BOTTOM_K_FINDER = "topKBottomKFinder";
        private static final String WINDOW_LENGTH = "windowLength";
        private static final String QUERY_SIZE = "querySize";
        private static final String COUNT = "count";
        private static final String EXPIRED_EVENT_CHUNK = "expiredEventChunk";

        private int windowLength;
        private int querySize;  // The K value
        private boolean outputExpectsExpiredEvents;

        private int count;  // The number of events in the batch
        private AbstractTopKBottomKFinder<Object> topKBottomKFinder;

        private ComplexEventChunk<StreamEvent> expiredEventChunk;

        private ExtensionState(int windowLength, int querySize, boolean outputExpectsExpiredEvents, int count,
                               ComplexEventChunk<StreamEvent> expiredEventChunk,
                               AbstractTopKBottomKFinder<Object> topKBottomKFinder) {
            this.windowLength = windowLength;
            this.querySize = querySize;
            this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
            this.count = count;
            this.expiredEventChunk = expiredEventChunk;
            this.topKBottomKFinder = topKBottomKFinder;
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
                if (outputExpectsExpiredEvents) {
                    return new HashMap<String, Object>() {
                        {
                            put(TOP_K_BOTTOM_K_FINDER, topKBottomKFinder);
                            put(WINDOW_LENGTH, windowLength);
                            put(QUERY_SIZE, querySize);
                            put(COUNT, count);
                            put(EXPIRED_EVENT_CHUNK, expiredEventChunk);
                        }
                    };
                } else {
                    return new HashMap<String, Object>() {
                        {
                            put(TOP_K_BOTTOM_K_FINDER, topKBottomKFinder);
                            put(WINDOW_LENGTH, windowLength);
                            put(QUERY_SIZE, querySize);
                            put(COUNT, count);
                        }
                    };
                }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void restore(Map<String, Object> state) {
                topKBottomKFinder = (AbstractTopKBottomKFinder<Object>) state.get(TOP_K_BOTTOM_K_FINDER);
                windowLength = (Integer) state.get(WINDOW_LENGTH);
                querySize = (Integer) state.get(QUERY_SIZE);
                count = (Integer) state.get(COUNT);

                if (state.size() == 5) {
                    expiredEventChunk = (ComplexEventChunk<StreamEvent>) state.get(EXPIRED_EVENT_CHUNK);
                }
        }
    }
}
