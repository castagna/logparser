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
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.castagna.logparser.Constants;
import com.cloudera.castagna.logparser.LogParser;

public class StatusCodesStatsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(StatusCodesStatsMapper.class);
    private static final LogParser parser = new LogParser();

	private Text outTextKey = new Text();
	private Text outTextValue = new Text();
	
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        log.debug("< ({}, {})", key, value);

        try {
			Map<String,String> logLine = parser.parseLine(value.toString());
			
			if ( logLine.get(LogParser.STATUS_CODE) != null ) {
				StringBuilder outKey = new StringBuilder();
				outKey.append(logLine.get(LogParser.TIME_YEAR));
				outKey.append(Constants.SEPARATOR);
				outKey.append(logLine.get(LogParser.TIME_MONTH));
				outKey.append(Constants.SEPARATOR);
				outKey.append(logLine.get(LogParser.TIME_DAY));
				outKey.append(Constants.SEPARATOR);
				outKey.append(logLine.get(LogParser.TIME_HOUR));
				outKey.append(Constants.SEPARATOR);
				outKey.append(logLine.get(LogParser.TIME_MINUTE));
//				outKey.append(Constants.SPACE);
//				outKey.append(logLine.get(LogParser.URL));
				
				StringBuilder outValue = new StringBuilder();
				outValue.append(logLine.get(LogParser.STATUS_CODE));
				outValue.append(Constants.COLON);
				outValue.append(Constants.ONE);
				
				outTextKey.clear();
				outTextKey.set(outKey.toString());
				
				outTextValue.clear();
				outTextValue.set(outValue.toString());

				context.write(outTextKey, outTextValue);
				log.debug("> ({}, {})", outTextKey, outTextValue);
			} else {
				// TODO
			}
		} catch (ParseException e) {
			log.debug ("Error parsing: {} {}", key, value);
		}
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
    
}
