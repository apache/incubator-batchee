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
# Modules
## jbatch (aka batchee-jbatch)
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-jbatch</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

Implements JBatch (aka JSR 352). More details on [configuration](./configuration.html) page.

## Hazelcast
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-hazelcast</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

A module based on Hazelcast API to allow distributed locks.


## GUI/Web module
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-servlet</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-jaxrs-client</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-jaxrs-server</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

A simple web front to visualize JBatch information and expose as JAX-RS resource JBatch `JobOperator`.

## Extras
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-extras</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

Basic implementations for Readers/Writers/Processors/.... More on it in extensions part.

## BeanIO
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-beanio</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

Basic implementations of a reader and a writer using BeanIO library. Details in extensions part.

## JSefa
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-jsefa</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

Basic implementations of a reader and a writer using JSefa library.

## Groovy
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-groovy</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

Basic implementations of a reader/processor/writer/batchlet delegating to a groovy script the processing. It allows
to add some dynamicity to batches.


## Camel
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-camel</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

A simple integration with Apache Camel.


## CDI
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-cdi</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

Provides basic batch oriented scopes (`@JobScoped` and `@StepScoped`).

## Maven Plugin
### Coordinates

<pre class="prettyprint linenums"><![CDATA[
<plugin>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-maven-plugin</artifactId>
  <version>${batchee.version}</version>
</plugin>
]]></pre>

### Goal

Allows you to contol your batchees from Maven.


## Jackson
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-jackson</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

Provides reader/writer based on jackson to read/write json.

## ModelMapper
### Dependency

<pre class="prettyprint linenums"><![CDATA[
<dependency>
  <groupId>org.apache.batchee</groupId>
  <artifactId>batchee-modelmapper</artifactId>
  <version>${batchee.version}</version>
</dependency>
]]></pre>

### Goal

Provides an `ItemProcessor` mapping input bean to another one based on `ModelMapper`.
