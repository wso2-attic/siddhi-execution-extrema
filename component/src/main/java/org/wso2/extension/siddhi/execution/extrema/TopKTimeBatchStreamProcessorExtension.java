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

import org.wso2.extension.siddhi.execution.extrema.util.AbstractTopKBottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.Constants;
import org.wso2.extension.siddhi.execution.extrema.util.TopKFinder;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;

/**
 * topKTimeBatch counts the frequency of different values of a specified attribute inside a time
 * window, and emits the highest (k) number of frequency values.
 */
@Extension(
        name = "topKTimeBatch",
        namespace = "extrema",
        description = "`topKTimeBatch` counts the frequency of different values of a specified " +
                "attribute within a time window, and emits the (k) number of values with the highest frequency.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the frequency is counted.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}),
                @Parameter(name = "time.window",
                        description = "The time window for which the frequency should be calculated.",
                        type = {DataType.INT, DataType.LONG}),
                @Parameter(name = "k.value",
                        description = "The number of frequent values that should be emitted as the output (e.g., if" +
                                " 2 is specified, this extension returns the two attribute values (for the specified" +
                                " attribute) that have the highest frequency.",
                        type = {DataType.INT})
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "topNElement",
                        description = "The value of the attribute that has the nth highest frequency. Here, N is an " +
                                "integer that can hold any value within the range, 1 <= N <= k.value, " +
                                "where 'k.value' is defines the number of frequent values that is required " +
                                "to be returned.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}
                ),
                @ReturnAttribute(
                        name = "topNFrequency",
                        description = "The frequency of the value of the attribute that has the nth highest " +
                                "frequency. Here, N is an integer that can hold any value within the range" +
                                " 1 <= N <= k.value, where 'k.value' defines the number of frequent values " +
                                "that is required to be returned.",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (item string, price long);\n" +
                                "from inputStream#extrema:topKTimeBatch(item, 1 sec,  3)\n" +
                                "insert all events into OutputStream;",
                        description =  "This query counts the frequency of the values for the `item` attribute within" +
                                " a time window of one second, and emits the 3 items. A batch of one second is" +
                                " collected. Once the window is full, the three items with the highest frequency are " +
                                "emitted and the window is reset."
                )
        }
)
public class TopKTimeBatchStreamProcessorExtension extends AbstractKTimeBatchStreamProcessorExtension {
    @Override
    protected AbstractTopKBottomKFinder<Object> createNewTopKBottomKFinder() {
        return new TopKFinder<Object>();
    }

    @Override
    protected String getExtensionNamePrefix() {
        return Constants.TOP_K_BOTTOM_K_TOP;
    }
}
