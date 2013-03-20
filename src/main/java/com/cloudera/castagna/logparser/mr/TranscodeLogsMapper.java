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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.castagna.logparser.Constants;
import com.cloudera.castagna.logparser.LogParser;

public class TranscodeLogsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final Logger log = LoggerFactory.getLogger(TranscodeLogsMapper.class);
    private static final LogParser parser = new LogParser();

	private Text outTextValue = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
	public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        log.debug("< ({}, {})", key, value);

        try {
			Map<String,String> logLine = parser.parseLine(value.toString());
			
			StringBuilder outValue = new StringBuilder();
			outValue.append(logLine.get(LogParser.REMOTE_HOSTNAME));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.USERNAME));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.HTTP_METHOD));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.URL));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.TIME_YEAR));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.TIME_MONTH));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.TIME_DAY));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.TIME_HOUR));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.TIME_MINUTE));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.TIME_SECOND));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.TIMESTAMP));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.STATUS_CODE));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.SIZE));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.ELAPSED_TIME));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.USER_AGENT));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get(LogParser.REFERER));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get("JSESSIONID"));
			outValue.append(Constants.TAB);
			outValue.append(logLine.get("SITESERVER"));
			outValue.append(Constants.TAB);
				
			outTextValue.clear();
			outTextValue.set(outValue.toString());

			context.write(NullWritable.get(), outTextValue);
			log.debug("> ({}, {})", NullWritable.get(), outTextValue);
		} catch (ParseException e) {
			log.debug ("Error parsing: {} {}", key, value);
		}
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
    
}
