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
import org.wso2.extension.siddhi.execution.extrema.util.BottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.Constants;

import java.util.List;

/**
 * bottomKLengthBatch counts the frequency of different values of a specified attribute inside
 * a batch window, and emits the lowest (k) number of frequency values.
 */
@Extension(
        name = "bottomKLengthBatch",
        namespace = "extrema",
        description = "`bottomKLengthBatch` counts the frequency of different values of a specified " +
                "attribute inside a batch window, and returns the number of least frequently occurring " +
                "values. The bottom K frequency values are returned per batch.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the frequency is counted.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}),
                @Parameter(name = "window.length",
                        description = "The length of the window observed.",
                        type = {DataType.INT}),
                @Parameter(name = "k.value",
                        description = "The number of bottom frequencies required.",
                        type = {DataType.INT})
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "bottomNElement",
                        description = "The value of the attribute that has the nth lowest frequency. Here, " +
                                "N is an integer that can hold any value within the range, 1 <= N <= k.value, where " +
                                "'k.value' is the function parameter that denotes the number of bottom " +
                                "frequencies considered.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}
                ),
                @ReturnAttribute(
                        name = "bottomNFrequency",
                        description = "The frequency of the value of the attribute that has the nth lowest " +
                                "frequency. Here, N is an integer that can hold any value within the range," +
                                " 1 <= N <= k.value.",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (item string, price long);\n" +
                                "\n" +
                                "from InputStream#extrema:bottomKLengthBatch(item, 6, 3)\n" +
                                "insert all events into OutputStream;",
                        description = "This function collects a batch of six events. Once the batch window is full, " +
                                "the three items with the lowest frequency are returned to the 'OutputStream'," +
                                " and the batch window is reset."
                )
        }
)
public class BottomKLengthBatchStreamProcessorExtension extends AbstractKLengthBatchStreamProcessorExtension {
    @Override
    protected AbstractTopKBottomKFinder<Object> createNewTopKBottomKFinder() {
        return new BottomKFinder<Object>();
    }

    @Override
    protected String getExtensionNamePrefix() {
        return Constants.TOP_K_BOTTOM_K_BOTTOM;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return attributeList;
    }
}
