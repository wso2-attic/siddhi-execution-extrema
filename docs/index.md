siddhi-execution-extrema
======================================

The **siddhi-execution-extrema extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that processes event streams based on different arithmetic properties.
Different types of processors are available to extract the extremas from the event streams according to the specified attribute in the stream.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-execution-extrema/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0">5.0.0</a>.

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

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#bottomk-stream-processor">bottomK</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>bottomK</code> counts the frequency of different values for a specified attribute, and returns the number of least frequently occurring values. The events are returned only if there is a change in the 'bottomK' results for each chunk of received events.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#bottomklengthbatch-stream-processor">bottomKLengthBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>bottomKLengthBatch</code> counts the frequency of different values of a specified attribute inside a batch window, and returns the number of least frequently occurring values. The bottom K frequency values are returned per batch.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#bottomktimebatch-stream-processor">bottomKTimeBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>bottomKTimeBatch</code> counts the frequency of different values for a specified attribute inside a time window, and outputs a specified number of least frequently occurring values. Events are output only if there is a change in the <code>bottomK</code> results for each chunk of received events.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#kalmanminmax-stream-processor">kalmanMinMax</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>kalmanMinMax</code> uses the Kalman filter to smooth the values of the time series in the given window size, and then determines the maxima and minima of that set of values. It returns the events with the minimum and/or maximum values for the specified attribute within the given window length, with the extrema type as <code>min</code> or <code>max</code> as relevant.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#kernelminmax-stream-processor">kernelMinMax</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>kernelMinMax</code> uses Gaussian Kernel to smooth values of the series within the given window size, and then determines the maxima and minima of that set of values. It returns the events with the minimum and/or maximum values for the specified attribute within the given window length, with the extrema type as <code>min</code> or <code>max</code> as relevant.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#minmax-stream-processor">minMax</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>minMax</code> finds the minimum and/or the maximum value within a given length window (maxPreBound+maxPostBound), where following conditions are met. <br><br>For minimum: <br>An event where the value for the specified attribute is greater by the percentage specified as the <code>preBoundChange</code> must have arrived within the <code>maxPreBound</code> length window before the event with the minimum value.<br>An event where the value for the specified attribute is greater by the percentage specified as the <code>postBoundChange</code> must have arrived within the <code>maxPostBound</code> length window after the event with the minimum value.<br><br>For maximum: <br>An event where the value for the specified attribute is less by the percentage specified as the <code>preBoundChange</code> must have arrived within the <code>maxPreBound</code> length window before the event with the maximum value.<br>An event where the value for the specified attribute is less by the percentage specified as the <code>postBoundChange</code> must have arrived within the <code>maxPreBound</code> length window after the event with the maximum value.<br><br>The extension returns the events with the minimum and/or maximum for the specified attribute within the given window length, with the extrema type as min or max as relevant. These events are returned with the following additional parameters.<br><code>preBound</code>: The actual distance between the minimum/maximum value and the threshold value. This value must be within the <code>MaxPreBound</code> window.<br>postBound: The actual distance between the minimum/maximum value and the threshold value. This value must be within the <code>MaxPostBound</code> window.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#topk-stream-processor">topK</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>topK</code> counts the frequency of different values of a specified attribute, and emits the (k) number of values with the highest frequency.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#topklengthbatch-stream-processor">topKLengthBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>topKLengthBatch</code> counts the frequency of different values of a specified attribute, within a batch window of a specified length, and emits the (k) number of values with the highest frequency.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#topktimebatch-stream-processor">topKTimeBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">Stream Processor</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>topKTimeBatch</code> counts the frequency of different values of a specified attribute within a time window, and emits the (k) number of values with the highest frequency.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#maxbylength-window">maxByLength</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>maxByLength</code> returns the event with the maximum value for the given attribute in the specified sliding window.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#maxbylengthbatch-window">maxByLengthBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>maxByLengthBatch</code> calculates and returns the maximum value of a specified attribute inside a batch window.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#maxbytime-window">maxbytime</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>maxbytime</code> calculates the maximum value of a specified attribute within a sliding time window and emits it. The output is updated for every event arrival and expiry during the <code>time.window.length</code> specified.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#maxbytimebatch-window">maxbytimebatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>maxbytimebatch</code> calculates the maximum value of a specified attribute within a time window, and emits it.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#minbylength-window">minByLength</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>minByLength</code> derives the minimum value for the given attribute in the specified sliding window.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#minbylengthbatch-window">minByLengthBatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>minByLengthBatch</code> calculates the minimum value of a specified attribute inside a batch window and emits it.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#minbytime-window">minbytime</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>minbytime</code> calculates the minimum value of a specified attribute within a sliding time window and emits it. The output is updated for every event arrival and expiry during the <code>time.window.length</code> specified.</p></p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-execution-extrema/api/5.0.0/#minbytimebatch-window">minbytimebatch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">Window</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;"><code>minbytimebatch</code> calculates the minimum value of a specified attribute within a time window, and emits it.</p></p></div>

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
