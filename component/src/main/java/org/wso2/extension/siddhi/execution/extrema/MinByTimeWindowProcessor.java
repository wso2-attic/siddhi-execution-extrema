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
 * from inputStream#window.minbymaxby:minbytime(attribute1,1 sec)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Description:
 * In the sample query given, 1 sec is the duration of the sliding window and attribute1 is the maxBy attribute.
 * According to the given attribute it will give the minimum event during last windowTime period
 * and gets updated on every event arrival and expiry.
 */
@Extension(
        name = "minbytime",
        namespace = "extrema",
        description = "`minbytime` calculates the minimum value of a specified attribute within a sliding time " +
                "window and emits it. The output is updated for every event arrival and expiry during " +
                "the `time.window.length` specified.",
        parameters = {
                @Parameter(name = "attribute",
                        description = "The attribute of which the minimum value is required.",
                        type = {DataType.INT, DataType.FLOAT, DataType.DOUBLE, DataType.LONG,
                                DataType.STRING}),
                @Parameter(name = "time.window.length",
                        description = "The length of the sliding time window observed.",
                        type = {DataType.INT, DataType.LONG})
        },
        examples = {
                @Example(
                        syntax = "define stream CseEventStream (symbol string, price float, volume int);\n" +
                                "from CseEventStream#window.extrema:minByTime(price, 1 sec) " +
                                "select symbol,price,volume\n" +
                                "insert into OutputStream;",
                        description =  "This query outputs the event with the minimum price for a sliding time " +
                                "window of one second. This output is updated with the arrival and expiry of " +
                                "every event (after one second of its arrival)."
                )
        }
)
public class MinByTimeWindowProcessor extends MaxByMinByTimeWindowProcessor {
    public MinByTimeWindowProcessor() {
        maxByMinByType = MaxByMinByConstants.MIN_BY;
        windowType = MaxByMinByConstants.MIN_BY_TIME;
    }
}
