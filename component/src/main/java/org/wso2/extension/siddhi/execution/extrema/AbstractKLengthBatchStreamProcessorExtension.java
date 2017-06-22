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

import org.wso2.extension.siddhi.execution.extrema.util.AbstractTopKBottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.Constants;
import org.wso2.extension.siddhi.execution.extrema.util.Counter;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

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
        extends StreamProcessor implements FindableProcessor {

    private static final String TOP_K_BOTTOM_K_FINDER = "topKBottomKFinder";
    private static final String WINDOW_LENGTH = "windowLength";
    private static final String QUERY_SIZE = "querySize";
    private static final String COUNT = "count";
    private static final String EXPIRED_EVENT_CHUNK = "expiredEventChunk";

    private int windowLength;
    private int querySize;          // The K value

    private int count;      // The number of events in the batch
    private VariableExpressionExecutor attrVariableExpressionExecutor;
    private AbstractTopKBottomKFinder<Object> topKBottomKFinder;

    private Object[] lastOutputData;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
        synchronized (this) {
            long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                // Current event arrival tasks
                if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                    topKBottomKFinder.offer(attrVariableExpressionExecutor.execute(streamEvent));
                    count++;
                }

                // Window end tasks
                if (count == windowLength) {
                    StreamEvent lastStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                    if (expiredEventChunk.getFirst() != null) {
                        // Adding expired events
                        if (outputExpectsExpiredEvents) {
                            expiredEventChunk.getFirst().setTimestamp(currentTime);
                            outputStreamEventChunk.add(expiredEventChunk.getFirst());
                            expiredEventChunk.clear();
                        }

                        // Adding the reset event
                        StreamEvent resetEvent = streamEventCloner.copyStreamEvent(lastStreamEvent);
                        resetEvent.setType(ComplexEvent.Type.RESET);
                        outputStreamEventChunk.add(resetEvent);
                    }

                    // Generating the list of additional attributes added to the events sent out
                    List<Counter<Object>> topKCounters = topKBottomKFinder.get(querySize);
                    Object[] outputStreamEventData = new Object[2 * querySize];
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
                        expiredEventChunk.add(expiredStreamEvent);
                    }

                    // Resetting window
                    topKBottomKFinder = createNewTopKBottomKFinder();
                    count = 0;
                }
            }
        }

        if (outputStreamEventChunk.getFirst() != null) {
            nextProcessor.process(outputStreamEventChunk);
        }
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition,
                                   ExpressionExecutor[] attributeExpressionExecutors,
                                   ConfigReader configReader,
                                   SiddhiAppContext siddhiAppContext) {
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
        return newAttributes;
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
    public Map<String, Object> currentState() {
        synchronized (this) {
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
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        synchronized (this) {
            topKBottomKFinder = (AbstractTopKBottomKFinder<Object>) state.get(TOP_K_BOTTOM_K_FINDER);
            windowLength = (Integer) state.get(WINDOW_LENGTH);
            querySize = (Integer) state.get(QUERY_SIZE);
            count = (Integer) state.get(COUNT);

            if (state.size() == 5) {
                expiredEventChunk = (ComplexEventChunk<StreamEvent>) state.get(EXPIRED_EVENT_CHUNK);
            }
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
}
