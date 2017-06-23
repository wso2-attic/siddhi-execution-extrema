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

/**
 * topKLengthBatch counts the frequency of different values of a specified attribute inside a
 * batch window, and emits the highest (k) number of frequency values.
 */
@Extension(
        name = "topKLengthBatch",
        namespace = "extrema",
        description = "topKLengthBatch counts the frequency of different values of a specified attribute" +
                " inside a batch window, and emits the highest (k) number of frequency values.",
        parameters = {},
        examples = {
                @Example(
                        syntax = "TBD",
                        description =  "TBD"
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
}
