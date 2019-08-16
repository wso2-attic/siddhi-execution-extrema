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
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;

import io.siddhi.query.api.definition.Attribute;
import org.wso2.extension.siddhi.execution.extrema.util.AbstractTopKBottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.Constants;
import org.wso2.extension.siddhi.execution.extrema.util.TopKFinder;

import java.util.List;

/**
 * topKLengthBatch counts the frequency of different values of a specified attribute inside a
 * batch window, and emits the highest (k) number of frequency values.
 */
@Extension(
        name = "topKLengthBatch",
        namespace = "extrema",
        description = "`topKLengthBatch` counts the frequency of different values of a specified attribute, " +
                "within a batch window of a specified length, and emits the (k) number of values with the highest" +
                " frequency.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the frequency is counted.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}),
                @Parameter(name = "window.length",
                        description = "The length of the window.",
                        type = {DataType.INT}),
                @Parameter(name = "k.value",
                        description = "The number of frequent values that should be emitted as the output (e.g., if" +
                                "2 is specified, this extension returns the two attribute values (for the specified" +
                                "attribute) that have the highest frequency.",
                        type = {DataType.INT})
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "topNElement",
                        description = "The value of the attribute that has the nth highest frequency. Here, N is an " +
                                "integer that can hold any value within the range, 1 <= N <= k.value, " +
                                "where 'k.value' is defined as the function parameter.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}
                ),
                @ReturnAttribute(
                        name = "topNFrequency",
                        description = "The frequency of the value of the attribute that has the nth highest " +
                                "frequency. Here, N is an integer that can hold values within the range," +
                                " of 1 <= N <= k.value, where k.value is defined as the function parameter.",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (item string, price long);\n" +
                                "\n" +
                                "from InputStream#extrema:topKLengthBatch(item, 6, 3)\n" +
                                "insert all events into OutputStream;",
                        description = "This query collects a batch of six events. Once the window is full, the three" +
                                " items with the highest frequency are emitted and the window is reset."
                )
        }
)

public class TopKLengthBatchStreamProcessorExtension extends AbstractKLengthBatchStreamProcessorExtension {
    @Override
    protected AbstractTopKBottomKFinder<Object> createNewTopKBottomKFinder() {
        return new TopKFinder<Object>();
    }

    @Override
    protected String getExtensionNamePrefix() {
        return Constants.TOP_K_BOTTOM_K_TOP;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributeList;
    }
}
