--
--
--     Licensed to the Apache Software Foundation (ASF) under one or more
--     contributor license agreements.  See the NOTICE file distributed with
--     this work for additional information regarding copyright ownership.
--     The ASF licenses this file to You under the Apache License, Version 2.0
--     (the "License"); you may not use this file except in compliance with
--     the License.  You may obtain a copy of the License at
--
--        http://www.apache.org/licenses/LICENSE-2.0
--
--     Unless required by applicable law or agreed to in writing, software
--     distributed under the License is distributed on an "AS IS" BASIS,
--     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--     See the License for the specific language governing permissions and
--     limitations under the License.
--

---
--- automatically executed by OpenEJB at started when creating jdbc/orderDB
---
CREATE TABLE Numbers (item  	INT, quantity  INT);
CREATE TABLE Orders (orderID	INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, itemID	INT, quantity  INT);
CREATE TABLE Inventory(itemID	INT NOT NULL PRIMARY KEY, quantity	INT NOT NULL );

INSERT INTO Inventory VALUES (1, 100);
INSERT INTO Numbers VALUES (1, 10);
INSERT INTO Numbers VALUES (2, 10);
INSERT INTO Numbers VALUES (3, 10);
INSERT INTO Numbers VALUES (4, 10);
INSERT INTO Numbers VALUES (5, 10);
INSERT INTO Numbers VALUES (6, 10);
INSERT INTO Numbers VALUES (7, 10);
INSERT INTO Numbers VALUES (8, 10);
INSERT INTO Numbers VALUES (9, 10);
INSERT INTO Numbers VALUES (10, 10);
INSERT INTO Numbers VALUES (11, 10);
INSERT INTO Numbers VALUES (12, 10);
INSERT INTO Numbers VALUES (13, 10);
INSERT INTO Numbers VALUES (14, 10);
INSERT INTO Numbers VALUES (15, 10);
INSERT INTO Numbers VALUES (16, 10);
INSERT INTO Numbers VALUES (17, 10);
INSERT INTO Numbers VALUES (18, 10);
INSERT INTO Numbers VALUES (19, 10);
INSERT INTO Numbers VALUES (20, 10);
