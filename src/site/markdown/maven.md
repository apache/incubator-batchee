<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Maven Plugin
## Coordinates

<pre class="prettyprint linenums"><![CDATA[
<plugin>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-maven-plugin</artifactId>
  <version>${batchee.version}</version>
</plugin>
]]></pre>

## Documentation generation parsing batch components

BatchEE provides a `doc` goal allowing to parse `BatchProperty` to generate your JBatch component documentation.

You can run it on a plain JBatch module but it is recommanded to add `doc-api` dependency to be able to use `@Documentation`
to describe what the property is used for:

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-doc-api</artifactId>
  <version>${batchee.version}</version>
</plugin>
]]></pre>

Then simply run:

<pre class="prettyprint"><![CDATA[
mvn batchee:doc
]]></pre>

## Configuration for a remote BatchEE instance (JAX-RS client)

<pre class="prettyprint linenums"><![CDATA[
<plugin>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-maven-plugin</artifactId>
  <version>0.0.1-SNAPSHOT</version>
  <configuration>
    <clientConfiguration>
      <baseUrl>http://localhost:8080/myapp/</baseUrl>
      <jsonProvider>org.apache.johnzon.jaxrs.JohnzonProvider</jsonProvider>
      <security>
        <type>Basic</type>
        <username>foo</username>
        <password>bar</password>
      </security>
      <ssl>
        <hostnameVerifier>org.MyHostVerifier</hostnameVerifier>
        <keystorePassword>xxx</keystorePassword>
        <keystoreType>JKS</keystoreType>
        <keystorePath>/c/cert.crt</keystorePath>
        <sslContextType>TLS</sslContextType>
        <keyManagerType>SunX509</keyManagerType>
        <keyManagerPath>/....</keyManagerPath>
        <trustManagerAlgorithm>...</trustManagerAlgorithm>
        <trustManagerProvider>...</trustManagerProvider>
        <hostnameVerifier>org.MyHostVerifier</hostnameVerifier>
      </ssl>
    </clientConfiguration>
  </configuration>
</plugin>
]]></pre>

## Goals

<pre class="prettyprint linenums"><![CDATA[
batchee:abandon
  Abandon a job.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId to abandon.
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:count-instance
  Count job instance.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    jobName
      the job name to use to count job instances
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:execution
  Print job an execution.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId to query.
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:executions
  Print job instance executions.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    instanceId
      the instanceId to use to query job executions
      Required: Yes

    jobName
      the job name to use to query job executions
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:help
  Display help information on batchee-maven-plugin.
  Call mvn batchee:help -Ddetail=true -Dgoal=<goal-name> to display parameter
  details.

  Available parameters:

    detail
      If true, display all settable properties for each goal.

    goal
      The name of the goal for which to show help. If unspecified, all goals
      will be displayed.

    indentSize
      The number of spaces per indentation level, should be positive.

    lineLength
      The maximum length of a display line, should be positive.

batchee:instance
  Print JobInstance for a particular execution.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId to use to find the corresponding job instance
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:instances
  Print job instances.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    count
      the maximum number of instance to bring back

    jobName
      the job name to use to find job instances
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

    start
      the first job instance to take into account

batchee:job-names
  List all executed job names.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:parameters
  Print parameters for a particular execution.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId to query to find parameters
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:restart
  Restart a job.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId representing the execution to restart
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jobParameters
      the job parameters to use.

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

    wait
      wait or not the end of this task before exiting maven plugin execution.

batchee:running
  List running executions.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    jobName
      the job name used to query running executions
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:start
  Start a job.

  Available parameters:

    additionalClasspathEntries
      manual entries added in the execution classpath

    clientConfiguration
      when executed remotely the client configuration

    jobName
      the job name of the job to start
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jobParameters
      the job parameters to use.

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

    useProjectClasspath
      if the project (binaries + dependencies) should be added during the
      execution to the classpath

    wait
      wait or not the end of this task before exiting maven plugin execution.

batchee:step-executions
  Print step executions of a job execution.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId used to find step executions.
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally

batchee:stop
  Stop a job.

  Available parameters:

    clientConfiguration
      when executed remotely the client configuration

    executionId
      the executionId of the execution to stop
      Required: Yes

    jobOperatorClass
      force to use a custom JobOperator

    jsonProvider
      The json provider to use to unmarshall responses in remote mode

    properties
      the BatchEE properties when executed locally
]]></pre>
