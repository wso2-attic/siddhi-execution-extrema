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

import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByConstants;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;

/**
 * Sample Query:
 * from inputStream#window.extrema:maxbytimebatch(attribute1,1 sec)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Description:
 * In the sample query given, 1 sec is the duration of the window and attribute1 is the maxBy attribute.
 * According to the given attribute it will give the maximum event within given time.
 */
@Extension(
        name = "maxbytimebatch",
        namespace = "extrema",
        description = "`maxbytimebatch` calculates the maximum value of a specified attribute within a time window, " +
                "and emits it.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the maximum value is required.",
                        type = {DataType.INT, DataType.FLOAT, DataType.DOUBLE, DataType.LONG,
                                DataType.STRING}),
                @Parameter(name = "time.batch.length",
                        description = "The length of the time window observed.",
                        type = {DataType.INT, DataType.LONG})
        },
        examples = {
                @Example(
                        syntax = "define stream CseEventStream (symbol string, price float, volume int);\n" +
                                "from CseEventStream#window.extrema:maxByTimeBatch(price, 1 sec) " +
                                "select symbol,price,volume\n" +
                                "insert into OutputStream ;",
                        description =  "This query considers a time-batch window of 1 second. After every second, " +
                                "the window is reset, and the event with the maximum price is output."

                )
        }
)
public class MaxByTimeBatchWindowProcessor extends MaxByMinByTimeBatchWindowProcessor {
    public MaxByTimeBatchWindowProcessor() {
        maxByMinByType = MaxByMinByConstants.MAX_BY;
        windowType = MaxByMinByConstants.MAX_BY_TIME_BATCH;
    }
}
