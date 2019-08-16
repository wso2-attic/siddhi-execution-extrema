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
import org.wso2.extension.siddhi.execution.extrema.util.AbstractTopKBottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.Constants;
import org.wso2.extension.siddhi.execution.extrema.util.Counter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sample Query (For topKLengthBatch implementation):
 * from inputStream#window.length(6)#extrema:topKTimeBatch(attribute1, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Sample Query (For bottomKLengthBatch implementation):
 * from inputStream#window.timeBatch(6)#extrema:bottomKTimeBatch(attribute1, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Description:
 * In the example query given, 3 is the k-value and attribute1 is the attribute of which the frequency is counted.
 * The frequencies of the values received for the attribute given will be counted by this
 * and the topK/bottomK values will be emitted
 * Events will be only emitted if there is a change in the topK/bottomK results for each
 * received chunk of events
 */
public abstract class AbstractKStreamProcessorExtension
        extends StreamProcessor<AbstractKStreamProcessorExtension.ExtensionState> {

    private VariableExpressionExecutor attrVariableExpressionExecutor;
    private List<Attribute> attributeList = new ArrayList<>();

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater,
                           ExtensionState state) {
        ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
        synchronized (this) {
            long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                // Current event arrival tasks
                if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                    state.lastStreamEvent = clonedStreamEvent;
                    state.topKBottomKFinder.offer(attrVariableExpressionExecutor.execute(clonedStreamEvent));
                } else if (streamEvent.getType() == ComplexEvent.Type.EXPIRED) {
                    state.topKBottomKFinder.offer(attrVariableExpressionExecutor.execute(clonedStreamEvent), -1);
                } else if (streamEvent.getType() == ComplexEvent.Type.RESET) {
                    // Setting the reset event to be used in the end of the window

                    // Resetting topK/bottomK finder for batch windows using RESET event
                    state.topKBottomKFinder = createNewTopKBottomKFinder();
                }
            }

            // Adding expired events
            while (state.expiredEventChunk.hasNext()) {
                StreamEvent expiredEvent = state.expiredEventChunk.next();
                expiredEvent.setTimestamp(currentTime);
                outputStreamEventChunk.add(expiredEvent);
            }
            state.expiredEventChunk.clear();

            // Adding the reset event
            if (state.lastStreamEvent != null) {
                StreamEvent resetEvent = streamEventCloner.copyStreamEvent(state.lastStreamEvent);
                resetEvent.setType(ComplexEvent.Type.RESET);
                outputStreamEventChunk.add(resetEvent);
            }

            // Adding the last event with the topK frequencies for the window
            List<Counter<Object>> topKCounters = state.topKBottomKFinder.get(state.querySize);
            Object[] outputStreamEventData = new Object[2 * state.querySize];
            boolean sendEvents = false;
            int i = 0;
            while (i < topKCounters.size()) {
                Counter<Object> topKCounter = topKCounters.get(i);
                outputStreamEventData[2 * i] = topKCounter.getItem();
                outputStreamEventData[2 * i + 1] = topKCounter.getCount();
                if (state.lastOutputData == null ||
                        state.lastOutputData[2 * i] != outputStreamEventData[2 * i] ||
                        state.lastOutputData[2 * i + 1] != outputStreamEventData[2 * i + 1]) {
                    sendEvents = true;
                }
                i++;
            }
            if (sendEvents) {
                state.lastOutputData = outputStreamEventData;
                complexEventPopulater.populateComplexEvent(state.lastStreamEvent, outputStreamEventData);
                outputStreamEventChunk.add(state.lastStreamEvent);

                // Setting the event expired in this window
                StreamEvent expiredStreamEvent = streamEventCloner.copyStreamEvent(state.lastStreamEvent);
                expiredStreamEvent.setType(ComplexEvent.Type.EXPIRED);
                state.expiredEventChunk.add(expiredStreamEvent);
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
        int querySize;
        ComplexEventChunk<StreamEvent> expiredEventChunk;
        AbstractTopKBottomKFinder<Object> topKBottomKFinder;
        if (this.attributeExpressionExecutors.length == 2) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
            topKBottomKFinder = createNewTopKBottomKFinder();
        } else {
            throw new SiddhiAppValidationException(
                    "2 arguments should be " + "passed to " + getExtensionNamePrefix() +
                            "KStreamProcessor, but found " + this.attributeExpressionExecutors.length
            );
        }

        // Checking the topK/bottomK attribute
        if (this.attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            attrVariableExpressionExecutor = (VariableExpressionExecutor) this.attributeExpressionExecutors[0];
        } else {
            throw new SiddhiAppValidationException(
                    "Attribute for ordering in " + getExtensionNamePrefix() +
                            "KStreamProcessor should be a variable. but found a constant attribute " +
                            this.attributeExpressionExecutors[1].getClass().getCanonicalName()
            );
        }

        // Checking the query size parameter
        if (this.attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = this.attributeExpressionExecutors[1].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                querySize = (Integer) ((ConstantExpressionExecutor) this.attributeExpressionExecutors[1]).getValue();
                if (querySize <= 0) {
                    throw new SiddhiAppValidationException(
                            "Query size parameter for " + getExtensionNamePrefix() +
                                    "KLengthBatchStreamProcessor should be greater than 0. but found " + attributeType
                    );
                }
            } else {
                throw new SiddhiAppValidationException(
                        "Query size parameter for " + getExtensionNamePrefix() +
                                "KStreamProcessor should be INT. but found " + attributeType
                );
            }
        } else {
            throw new SiddhiAppValidationException(
                    "Query size parameter for " + getExtensionNamePrefix() +
                            "KStreamProcessor should be a constant. but found a dynamic attribute " +
                            this.attributeExpressionExecutors[2].getClass().getCanonicalName()
            );
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
        return () -> new ExtensionState(querySize, outputExpectsExpiredEvents, null,
                null, topKBottomKFinder, expiredEventChunk);
    }

    @Override
    public void start() {
        // Do nothing
    }

    @Override
    public void stop() {
        // Do nothing
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
     * Create and return either a TopKFinder or a BottomKFinder
     * Should be implemented by the child classes which will determine whether it is top K or bottom K
     *
     * @return TopKFinder or BottomKFinder
     */
    protected abstract AbstractTopKBottomKFinder<Object> createNewTopKBottomKFinder();

    /**
     * Return the name prefix that should be used in the returning extra parameters and in the
     * exception that might get thrown should be either "Top" or "Bottom" to indicate whether
     * it is top K or bottom K
     *
     * @return Name prefix. Should be either "Top" or "Bottom"
     */
    protected abstract String getExtensionNamePrefix();

    static class ExtensionState extends State {
        private static final String TOP_K_BOTTOM_K_FINDER = "topKBottomKFinder";
        private static final String QUERY_SIZE = "querySize";
        private static final String LAST_STREAM_EVENT = "lastStreamEvent";
        private static final String LAST_OUTPUT_DATA = "lastOutputData";
        private static final String EXPIRED_EVENT_CHUNK = "expiredEventChunk";

        private int querySize; // The K value
        private boolean outputExpectsExpiredEvents;
        private AbstractTopKBottomKFinder<Object> topKBottomKFinder;

        private Object[] lastOutputData;
        // Event used for adding the topK/bottomK items and frequencies added to it
        private StreamEvent lastStreamEvent;
        private ComplexEventChunk<StreamEvent> expiredEventChunk;

        private ExtensionState(int querySize, boolean outputExpectsExpiredEvents, Object[] lastOutputData,
                               StreamEvent lastStreamEvent, AbstractTopKBottomKFinder<Object> topKBottomKFinder,
                               ComplexEventChunk<StreamEvent> expiredEventChunk) {
            this.querySize = querySize;
            this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
            this.lastOutputData = lastOutputData;
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
                            put(QUERY_SIZE, querySize);
                            put(LAST_STREAM_EVENT, lastStreamEvent);
                            put(LAST_OUTPUT_DATA, lastOutputData);
                            put(EXPIRED_EVENT_CHUNK, expiredEventChunk);
                        }
                    };
                } else {
                    return new HashMap<String, Object>() {
                        {
                            put(TOP_K_BOTTOM_K_FINDER, topKBottomKFinder);
                            put(QUERY_SIZE, querySize);
                            put(LAST_STREAM_EVENT, lastStreamEvent);
                            put(LAST_OUTPUT_DATA, lastOutputData);
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
                querySize = (Integer) state.get(QUERY_SIZE);

                lastStreamEvent = (StreamEvent) state.get(LAST_STREAM_EVENT);
                lastOutputData = (Object[]) state.get(LAST_OUTPUT_DATA);
                if (state.size() == 5) {
                    expiredEventChunk = (ComplexEventChunk<StreamEvent>) state.get(EXPIRED_EVENT_CHUNK);
                }
            }
        }
    }
}
