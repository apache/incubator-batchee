/**
 * Copyright 2012 International Business Machines Corp.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.batchee.container.modelresolver.impl;

import org.apache.batchee.jaxb.ExceptionClassFilter;

import java.util.Properties;


public class ExceptionClassesPropertyResolver extends AbstractPropertyResolver<ExceptionClassFilter> {

    public ExceptionClassesPropertyResolver(boolean isPartitionStep) {
        super(isPartitionStep);
    }

    @Override
    public ExceptionClassFilter substituteProperties(ExceptionClassFilter exceptionClassFilter,
                                                     Properties submittedProps, Properties parentProps) {
    
		/*
        <xs:complexType name="ExceptionClassFilter">
            <xs:sequence>
                <xs:element name="include" minOccurs="0" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence />
                        <xs:attribute name="class" use="required" type="xs:string" />
                    </xs:complexType>
                </xs:element>
                <xs:element name="exclude" minOccurs="0" maxOccurs="unbounded">
                    <xs:complexType>
                        <xs:sequence />
                        <xs:attribute name="class" use="required" type="xs:string" />
                    </xs:complexType>
                </xs:element>
            </xs:sequence>
        </xs:complexType>
		*/

        // Resolve ExceptionClassFilter properties
        if (exceptionClassFilter.getIncludeList() != null) {
            for (final ExceptionClassFilter.Include includeElem : exceptionClassFilter.getIncludeList()) {
                includeElem.setClazz(this.replaceAllProperties(includeElem.getClazz(), submittedProps, parentProps));
            }

            for (final ExceptionClassFilter.Exclude excludeElem : exceptionClassFilter.getExcludeList()) {
                excludeElem.setClazz(this.replaceAllProperties(excludeElem.getClazz(), submittedProps, parentProps));
            }

        }

        return exceptionClassFilter;


    }

}
