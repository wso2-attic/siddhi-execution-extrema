siddhi-execution-extrema
======================================

The **siddhi-execution-extrema extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that processes event streams based on different arithmetic properties.
Different types of processors are available extract the extrema from the event streams based on the specified attribute in the stream.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT">4.0.2-SNAPSHOT</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.execution.extrema</groupId>
        <artifactId>siddhi-execution-extrema</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-extrema/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-execution-extrema/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#minbytimebatch-window">minbytimebatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>Calculates the minimum value of a specified attribute within a time window, and emits it.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#minbylength-window">minByLength</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>The event with the minimum value for the given attribute in the specified sliding window is emitted.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#minbylengthbatch-window">minByLengthBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>Calculates the minimum value of a specified attribute inside a batch window and emits it.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#maxbylength-window">maxByLength</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>The event with the maximum value for the given attribute in the specified sliding window is emitted.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#maxbytimebatch-window">maxbytimebatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>Calculates the maximum value of a specified attribute within a time window, and emits it.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#minbytime-window">minbytime</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>Calculates the minimum value of a specified attribute within a sliding time window and emits it. The output is updated for every event arrival and expiry during the time.window.length specified.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#maxbytime-window">maxbytime</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>Calculates the maximum value of a specified attribute within a sliding time window and emits it. The output is updated for every event arrival and expiry during the time.window.length specified.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#maxbylengthbatch-window">maxByLengthBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>Calculates the maximum value of a specified attribute inside a batch window and emits it.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#kernelminmax-stream-processor">kernelMinMax</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>kernalMinMax uses Gaussian Kernel to smooth the time series values in the given window size, and then determine the maxima and minima of that set of values. Returns the events with the minimum and/or maximum for the specified attribute within the given window length, with the extrema type as min or max as relevant.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#topk-stream-processor">topK</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>topK counts the frequency of different values of a specified attribute, and emits the highest (k) number of frequency values.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#bottomktimebatch-stream-processor">bottomKTimeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>bottomKTimeBatch counts the frequency of different values of a specified attribute inside a time window, and emits the lowest (k) number of frequency values. Events are emitted only if there is a change in the bottomK results for each received chunk of events.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#topktimebatch-stream-processor">topKTimeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>topKTimeBatch counts the frequency of different values of a specified attribute inside a time window, and emits the highest (k) number of frequency values.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#bottomk-stream-processor">bottomK</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>bottomK counts the frequency of different values of a specified attribute, and emits the lowest (k) number of frequency values. Events are emitted only if there is a change in the bottomK results for each received chunk of events.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#minmax-stream-processor">minMax</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>minMax finds the minimum and/or the maximum value within a given length window (maxPreBound+maxPostBound), where following conditions are met. <br><br>For minimum: <br>An event where the value for the specified attribute is greater by the percentage specified as the preBoundChange should have arrived prior to the event with the minimum value, within the maxPreBound length window.<br>An event where the value for the specified attribute is greater by the percentage specified as the postBoundChange should have arrived after the event with the minimum value, within the maxPostBound length window.<br><br>For maximum: <br>An event where the value for the specified attribute is less by the percentage specified as the preBoundChange should have arrived prior to the event with the maximum value, within the maxPreBound length window.<br>An event where the value for the specified attribute is less by the percentage specified as the postBoundChange should have arrived after the event with the maximum value, within the maxPostBound length window.<br><br>Returns the events with the minimum and/or maximum for the specified attribute within the given window length, with the extrema type as min or max as relevant. These events are returned with the following additional parameters.<br>preBound: The actual distance between the minimum/maximum value and the threshold value. This value should be within the MaxPreBound window.<br>postBound: The actual distance between the minimum/maximum value and the threshold value. This value should be within the MaxPostBound window.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#topklengthbatch-stream-processor">topKLengthBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>topKLengthBatch counts the frequency of different values of a specified attribute inside a batch window, and emits the highest (k) number of frequency values.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#kalmanminmax-stream-processor">kalmanMinMax</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>The kalmanMinMax function uses the kalman filter to smooth the time series values in the given window size, and then determine the maxima and minima of that set of values. Returns the events with the minimum and/or maximum for the specified attribute within the given window length, with the extrema type as min or max as relevant.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/4.0.2-SNAPSHOT/#bottomklengthbatch-stream-processor">bottomKLengthBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>bottomKLengthBatch counts the frequency of different values of a specified attribute inside a batch window, and emits the lowest (k) number of frequency values. The bottom K frequency values are emitted per batch.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
