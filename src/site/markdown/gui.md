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
# GUI

Apache BatchEE provides a Web-UI and 2 different ways to expose Batch details via REST

## SimpleRest 

For a quick out of the box solution to gather information about the JBatch environment you can use our Servlet based solution


## JAX-RS resource

`org.apache.batchee.jaxrs.server.JBatchResourceImpl` maps more or less `javax.batch.operations.JobOperator` API
to JAXRS. It is available in `batchee-jaxrs-server` module.

To define it with CXF you can use the `CXFNonSpringServlet` in a servlet container, in a JavaEE container
you surely already have it and just need to define a custom `javax.ws.rs.core.Application` with `JBatchResource`
as class in `getClasses` and configure `org.apache.batchee.jaxrs.server.JBatchExceptionMapper` if you want
to map `javax.batch.operations.BatchRuntimeException` to status 500:

<pre class="prettyprint linenums"><![CDATA[
<web-app version="2.5"
         xmlns="http://java.sun.com/xml/ns/javaee"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/web-app_2_5.xsd">
  <servlet>
    <servlet-name>CXFServlet</servlet-name>
    <display-name>JBatch JAXRS Servlet</display-name>
    <servlet-class>org.apache.cxf.jaxrs.servlet.CXFNonSpringJaxrsServlet</servlet-class>
    <init-param>
      <param-name>jaxrs.serviceClasses</param-name>
      <param-value>org.apache.batchee.jaxrs.server.JBatchResourceImpl</param-value>
    </init-param>
    <init-param>
      <param-name>jaxrs.providers</param-name>
      <param-value>
        org.apache.batchee.jaxrs.common.johnzon.JohnzonBatcheeProvider,
        org.apache.batchee.jaxrs.server.JBatchExceptionMapper
      </param-value>
    </init-param>
    <init-param>
      <param-name>jaxrs.extensions</param-name>
      <param-value>json=application/json</param-value>
    </init-param>
    <load-on-startup>1</load-on-startup>
  </servlet>

  <servlet-mapping>
    <servlet-name>CXFServlet</servlet-name>
    <url-pattern>/api/*</url-pattern>
  </servlet-mapping>
</web-app>
]]></pre>

Note: instead of johnzon you can also use jackson as JAX-RS Json provider: `com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider`.
Note: `JohnzonBatcheeProvider` is exactly the same as `org.apache.johnzon.jaxrs.JohnzonProvider` provider with `org.apache.batchee.jaxrs.common.johnzon.TimestampAdapter` registered
to convert dates to timestamps. You can use Johnzon provider directly as well but the date conversion will be Johnzon one.

Here is the mapping:

* /job-names
* /job-instance/count/{name}
* /job-instances/{name}?start={start}&count={count}
* /executions/running/{name}
* /execution/parameter/{id}
* /job-instance/{id}
* /job-executions/{id}/{name}
* /job-execution/{id}
* /step-executions/{id}
* /execution/start/{name}
* /execution/restart/{id}
* /execution/stop/{id}
* /execution/abandon/{id}

Note: `batchee-jaxrs-client` provides a way to query it through the `JobOperator` API. You need to use
`org.apache.batchee.jaxrs.client.BatchEEJAXRSClientFactory.newClient(String url, Class<?> jsonProvider, API apiType)`.
API.AUTO tries to use JAXRS 2 client and if not available uses cxf 2.6 clients. In this last case you need to provide `cxf-rt-frontend-jaxrs`.

## HTML gui

It is based on `org.apache.batchee.servlet.JBatchController` but since the jar - `batchee-servlet` - is in a webapp in a servlet 3.0 container,
it is automatically added and you don't need to define it in your `web.xml`.

The configuration through init parameters is:

* org.apache.batchee.servlet.active: boolean to deactivate it
* org.apache.batchee.servlet.mapping: mapping for the gui, default /jbatch/*
* org.apache.batchee.servlet.filter.private: boolean saying if internal jsp should be protected, it adds a filter to check URLs on each request
