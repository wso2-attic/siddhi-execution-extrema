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
import org.wso2.siddhi.annotation.util.DataType;

/**
 * topKTimeBatch counts the frequency of different values of a specified attribute inside a time
 * window, and emits the highest (k) number of frequency values.
 */
@Extension(
        name = "topKTimeBatch",
        namespace = "extrema",
        description = "topKTimeBatch counts the frequency of different values of a specified attribute " +
                "inside a time window, and emits the highest (k) number of frequency values.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the frequency is counted.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}),
                @Parameter(name = "time.window",
                        description = "The time window during which the frequency should be calculated.",
                        type = {DataType.INT, DataType.LONG}),
                @Parameter(name = "k.value",
                        description = "The number of top frequencies required.",
                        type = {DataType.INT})
        },
        examples = {
                @Example(
                        syntax = "define stream inputStream (item string, price long);\n" +
                                "from inputStream#extrema:topKTimeBatch(item, 1 sec,  3)\n" +
                                "insert all events into outputStream;",
                        description =  "In the given example query, a batch of 1 second will be collected. " +
                                "Once the window is full, the 3 items with the highest frequency will be " +
                                "emitted out and the window will be reset."
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
