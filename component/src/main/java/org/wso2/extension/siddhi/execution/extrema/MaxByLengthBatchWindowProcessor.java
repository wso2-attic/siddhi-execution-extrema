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
import io.siddhi.annotation.util.DataType;
import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByConstants;

/**
 * Sample Query:
 * <p>
 * from inputStream#window.minbymaxby:maxByLengthBatch(attribute1, 4)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Description:
 * In the example query given, 4 is the number of events the window should tumble and attribute1 is the attribute
 * that need to be compared to find the event which has max value.
 * According to the given attribute it will give event which holds the maximum value.
 */
@Extension(
        name = "maxByLengthBatch",
        namespace = "extrema",
        description = "`maxByLengthBatch` calculates and returns the maximum value of a specified attribute inside a " +
                "batch window.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the maximum value is required.",
                        type = {DataType.INT, DataType.FLOAT, DataType.DOUBLE, DataType.LONG,
                                DataType.STRING}),
                @Parameter(name = "batch.length",
                        description = "The length of the batch observed.",
                        type = {DataType.INT})
        },
        examples = {
                @Example(
                        syntax = "define stream CseEventStream (symbol string, price float, volume int);\n" +
                                "\n" +
                                "from CseEventStream#window.extrema:maxByLengthBatch(price, 4) \n" +
                                "select symbol,price,volume\n" +
                                "insert into OutputStream;",
                        description =  "This query collects a batch of 4 events. Once the window is full, the item " +
                                "with the maximum price in the batch is returned as the output, and the window is " +
                                "reset."
                )
        }
)
public class MaxByLengthBatchWindowProcessor extends MaxByMinByLengthBatchWindowProcessor {

    public MaxByLengthBatchWindowProcessor() {
        super.minByMaxByExecutorType = MaxByMinByConstants.MAX_BY;
    }

}
