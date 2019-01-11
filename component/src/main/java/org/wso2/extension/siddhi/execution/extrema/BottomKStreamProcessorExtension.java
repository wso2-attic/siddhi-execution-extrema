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
import org.wso2.extension.siddhi.execution.extrema.util.BottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.Constants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;

/**
 * bottomK counts the frequency of different values of a specified attribute,
 * and emits the lowest (k) number of frequency values.
 */
@Extension(
        name = "bottomK",
        namespace = "extrema",
        description = "`bottomK` counts the frequency of different values for a specified attribute, " +
                "and returns the number of least frequently occurring values. The events are returned only if " +
                "there is a change in the 'bottomK' results for each chunk of received events.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the frequency is counted.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}),
                @Parameter(name = "k.value",
                        description = "The number of lowest frequencies required.",
                        type = {DataType.INT})
        },
        returnAttributes = {
                @ReturnAttribute(
                        name = "bottomNElement",
                        description = "The value of the attribute that has the nth lowest frequency. Here, " +
                                "N is an integer that can hold any value within the range," +
                                " 1 <= N <= k.value, where 'k.value' " +
                                "defines the number of attributes that are required to be returned as " +
                                "the ones with the lowest frequencies.",
                        type = {DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT}
                ),
                @ReturnAttribute(
                        name = "bottomNFrequency",
                        description = "The frequency of the value of the attribute that has the nth lowest " +
                                "frequency. Here, N is an integer that can hold any value within the range," +
                                " 1 <= N <= k.value, where 'k.value' defines the number of attributes that are " +
                                "required to be returned as the ones with the lowest frequencies.",
                        type = {DataType.LONG}
                )
        },
        examples = {
                @Example(
                        syntax = "define stream InputStream (item string, price long);\n" +
                                "\n" +
                                "from InputStream#extrema:bottomK(item, 3)\n" +
                                "insert all events into OutputStream;",
                        description =  "This query returns the three items with the lowest frequency counts."
                )
        }
)
public class BottomKStreamProcessorExtension extends AbstractKStreamProcessorExtension {
    @Override
    protected AbstractTopKBottomKFinder<Object> createNewTopKBottomKFinder() {
        return new BottomKFinder<Object>();
    }

    @Override
    protected String getExtensionNamePrefix() {
        return Constants.TOP_K_BOTTOM_K_BOTTOM;
    }
}
