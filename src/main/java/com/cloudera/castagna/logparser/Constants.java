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

package com.cloudera.castagna.logparser;


public class Constants {

	public static final String SEPARATOR = "-";
	public static final String COLON = ":";
	public static final String SPACE = " ";
	public static final String TAB = "\t";
	public static final String ONE = "1";

	public static final String OPTION_USE_COMPRESSION = "useCompression";
	public static final boolean OPTION_USE_COMPRESSION_DEFAULT = false;

	public static final String OPTION_OVERWRITE_OUTPUT = "overwriteOutput";
	public static final boolean OPTION_OVERWRITE_OUTPUT_DEFAULT = false;

	public static final String OPTION_RUN_LOCAL = "runLocal";
	public static final boolean OPTION_RUN_LOCAL_DEFAULT = false;

	public static final String OPTION_NUM_REDUCERS = "numReducers";
	public static final int OPTION_NUM_REDUCERS_DEFAULT = 20;

	public static final String STATUS_CODES_STATS = "Status Codes Stats";
	public static final String TRANSCODE_LOGS = "Transcode Logs";

}
