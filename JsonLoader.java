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

package org.apache.pig.piggybank.storage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Reads and parses JSON from a json file.
 *
 * Usage:
 * e.g. JSON that looks like:
 * --------------------------------------------------------------
 * {"menu": {
 *    "id": "file",
 *    "value": "File",
 *    "popup": {
 *      "menuitem": [
 *        {"value": "New", "onclick": "CreateNewDoc()"},
 *        {"value": "Open", "onclick": "OpenDoc()"},
 *        {"value": "Close", "onclick": "CloseDoc()"}
 *      ]
 *    }
 * }}
 * --------------------------------------------------------------
 *  register the jar containing this class (e.g. piggybank.jar)
 *  a = load '/tmp/jsontest' using org.pig.piggybank.storage.JsonLoader() as (json:map[]);
 *  b = foreach a generate flatten(json#'menu') as menu;
 *  c = foreach b generate flatten(menu#'popup') as popup;
 *  d = foreach c generate flatten(popup#'menuitem') as menu;
 *  e = foreach d generate flatten(men#'value') as val;
 *
 */
public class JsonLoader extends LoadFunc {
	private static final TupleFactory tupleFactory = TupleFactory.getInstance();
	private ObjectMapper mapper;
	private LineRecordReader in = null;

	public JsonLoader() {
        super();
        mapper = new ObjectMapper();
	}

	@SuppressWarnings("unchecked")
	@Override
	public InputFormat getInputFormat() throws IOException {
		return new PigTextInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		boolean notDone = in.nextKeyValue();
		if (!notDone) {
			return null;
		}
		Text val = in.getCurrentValue();
		if (val == null) {
			return null;
		}
        String line = val.toString();
		if (line.length() > 0) {
			Tuple t = parseStringToTuple(line);
            if (t != null) {
				return t;
			}
		}
        return null;
	}

    protected Tuple parseStringToTuple(String line) {
        try {
            Map<String, Object> values = new HashMap<String, Object>();
            JsonNode node = mapper.readTree(line);
            System.out.println(mapper.readTree(line));
            flatten_value(node, values);
            return tupleFactory.newTuple(values);
        } catch (NumberFormatException e) {
            e.getMessage().concat("Very big number exceeds the scale of long: " + line);
            return null;
        } catch (JsonParseException e) {
            e.getMessage().concat("Could not json-decode string: " + line);
            return null;
        } catch (IOException e) {
            e.getMessage();
            return null;
        }
    }

	private void flatten_value(JsonNode node, Map<String, Object> values) {
        Iterator<String> keys = node.getFieldNames();
        Iterator<JsonNode> nodes = node.getElements();
        while (keys.hasNext()) {
            String key = keys.next();
            JsonNode value = nodes.next();

            System.out.println(key + ":" + value.toString());
            if (value.isArray()) {
                ArrayNode array = (ArrayNode) value;
                DataBag bag = DefaultBagFactory.getInstance().newDefaultBag();
                for (JsonNode innervalue : array) {
                    flatten_array(innervalue, bag);
                }
                values.put(key, bag);
            } else if (value.isObject()) {
                Map<String, Object> values2 = new HashMap<String, Object>();
                flatten_value((ObjectNode) value, values2);
                values.put(key, tupleFactory.newTuple(values2));
            } else {
                values.put(key, value != null ? value.toString().replaceAll("[\"]", "") : null);
            }
        }
	}

	private void flatten_array(JsonNode value, DataBag bag) {
		if(value.isArray()) {
			ArrayNode array = (ArrayNode)value;
			DataBag b = DefaultBagFactory.getInstance().newDefaultBag();
			for(JsonNode innervalue :array) {
				flatten_array(innervalue, b);
			}
			bag.addAll(b);
		} else if (value.isObject()){
			Map<String, Object> values2 = new HashMap<String, Object>();
			flatten_value((ObjectNode)value, values2);
			bag.add(tupleFactory.newTuple(values2));
		} else {
			if(value !=null) {
				bag.add( tupleFactory.newTuple(value));
			}
		}
    }

	@SuppressWarnings("unchecked")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
		in = (LineRecordReader) reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		 PigFileInputFormat.setInputPaths(job, location);
	}
}