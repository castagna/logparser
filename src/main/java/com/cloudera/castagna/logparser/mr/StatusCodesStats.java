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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.castagna.logparser.Constants;
import com.cloudera.castagna.logparser.Utils;

public class StatusCodesStats extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(StatusCodesStats.class);

	public StatusCodesStats() {
		super();
        log.debug("constructed with no configuration.");
	}
	
	public StatusCodesStats(Configuration configuration) {
		super(configuration);
        log.debug("constructed with configuration.");
	}
    
	@Override
	public int run(String[] args) throws Exception {
		if ( args.length != 2 ) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration configuration = getConf();
        boolean useCompression = configuration.getBoolean(Constants.OPTION_USE_COMPRESSION, Constants.OPTION_USE_COMPRESSION_DEFAULT);

        if ( useCompression ) {
            configuration.setBoolean("mapred.compress.map.output", true);
    	    configuration.set("mapred.output.compression.type", "BLOCK");
    	    configuration.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        }

        boolean overrideOutput = configuration.getBoolean(Constants.OPTION_OVERWRITE_OUTPUT, Constants.OPTION_OVERWRITE_OUTPUT_DEFAULT);
        FileSystem fs = FileSystem.get(new Path(args[1]).toUri(), configuration);
        if ( overrideOutput ) {
            fs.delete(new Path(args[1]), true);
        }

		Job job = Job.getInstance(configuration);
		job.setJobName(Constants.STATUS_CODES_STATS);
		job.setJarByClass(getClass());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(StatusCodesStatsMapper.class);		    
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setCombinerClass(StatusCodesStatsCombiner.class);
		
		job.setReducerClass(StatusCodesStatsReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		Utils.setReducers(job, configuration, log);

       	job.setOutputFormatClass(TextOutputFormat.class);

       	if ( log.isDebugEnabled() ) Utils.log(job, log);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
	    log.debug("main method: {}", Utils.toString(args));
		int exitCode = ToolRunner.run(new Configuration(), new StatusCodesStats(), args);
		System.exit(exitCode);
	}
	
}
