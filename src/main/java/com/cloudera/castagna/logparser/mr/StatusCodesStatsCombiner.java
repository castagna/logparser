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

package com.cloudera.castagna.logparser.mr;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.castagna.logparser.Constants;
import com.cloudera.castagna.logparser.Utils;

public class StatusCodesStatsCombiner extends Reducer<Text, Text, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(StatusCodesStatsCombiner.class);

	private Text outTextValue = new Text();
    
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Map<String,Integer> counters = new TreeMap<String,Integer>();
		Iterator<Text> iter = values.iterator();
		while ( iter.hasNext() ) {
			Text value = iter.next();
			log.debug("< ({}, {})", key, value);	
			Utils.increment (counters, value);
		}
		
		StringBuilder outValue = new StringBuilder();
		
		for (String k : counters.keySet()) {
			outValue.append(k);
			outValue.append(Constants.COLON);
			outValue.append(counters.get(k));
			outValue.append(Constants.SPACE);
		}

		outTextValue.clear();
		outTextValue.set(outValue.toString());

		context.write(key, outTextValue);
		log.debug("> ({}, {})", key, outTextValue);			
	}
    
}
