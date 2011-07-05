/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.piggybank.test.storage;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.MiniCluster;
import org.apache.pig.test.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestJsonLoader {
    private ExecType execType = ExecType.LOCAL;
    private MiniCluster cluster = MiniCluster.buildCluster();
    private static PigServer pig;
    private static final String datadir = "build/test/tmpdata/";
    private PigContext pigContext = new PigContext(execType, new Properties());

    @Before
    public void setUp() throws IOException {
        pig = new PigServer(execType, cluster.getProperties());
        Util.deleteDirectory(new File(datadir));
        pig.mkdirs(datadir);
        createJsonInputFile();
    }

    private void createJsonInputFile() throws IOException {
        Util.createLocalInputFile(datadir + "originput", new String[]{
                "{\"thing\":\"value\",\"menu\":{\"id\":\"file\",\"value\":\"File\",\"popup\":{\"menuitem\":" +
                "[{\"value\":\"New\",\"onclick\":\"CreateNewDoc()\"},{\"value\":\"Open\",\"onclick\":\"OpenDoc()\"}," +
                "{\"value\":\"Close\",\"onclick\":\"CloseDoc()\"}]}}}"}
        );
    }

    @After
    public void tearDown() {
        Util.deleteDirectory(new File(datadir));
        pig.shutdown();
    }

    @Test
    public void test_JsonLoader_Parses_Top_Level_Field() throws IOException {
        pigContext.connect();
        pig.registerQuery("A = LOAD '" + datadir + "originput' using org.apache.pig.piggybank.storage.JsonLoader() " +
                "as (json:map[]);");
        pig.registerQuery("B = foreach A generate flatten(json#'thing') as thing;");
        TupleFactory tupleFactory = TupleFactory.getInstance();
        Tuple expectedTuple = tupleFactory.newTuple("value");
        Iterator<Tuple> iterator = pig.openIterator("B");
        while (iterator.hasNext()) {
            assertEquals(expectedTuple.toString(), iterator.next().toString());
        }
    }

    @Test
    public void test_JsonLoader_Parses_Deeply_Nested_Json_Field() throws IOException {
        pigContext.connect();
        pig.registerQuery("a = LOAD '" + datadir + "originput' using org.apache.pig.piggybank.storage.JsonLoader() " +
                "as (json:map[]);");
        pig.registerQuery("b = foreach a generate flatten(json#'menu') as menu;");
        pig.registerQuery("c = foreach b generate flatten(menu#'popup') as popup;");
        pig.registerQuery("d = foreach c generate flatten(popup#'menuitem') as menuitem;");
        pig.registerQuery("e = foreach d generate flatten(menuitem#'value') as val;");
        List<Tuple> expectedResults = buildExpectedNestedJsonResults();
        Iterator<Tuple> iterator = pig.openIterator("e");
        int counter = 0;
        while (iterator.hasNext()) {
            assertEquals(expectedResults.get(counter++).toString(), iterator.next().toString());
        }
    }

    private List<Tuple> buildExpectedNestedJsonResults() {
        List<Tuple> expectedResults = new LinkedList<Tuple>();
        TupleFactory tupleFactory = TupleFactory.getInstance();
        expectedResults.add(tupleFactory.newTuple("New"));
        expectedResults.add(tupleFactory.newTuple("Open"));
        expectedResults.add(tupleFactory.newTuple("Close"));
        return expectedResults;
    }
}
