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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// stolen/inspired from: http://svn.apache.org/repos/asf/gora/trunk/gora-tutorial/src/main/java/org/apache/gora/tutorial/log/LogManager.java

public class LogParser {

	public static final String PORT = "port";
	public static final String REFERER = "referer";
	public static final String SET_COOKIES = "set_cookies";
	public static final String COOKIES = "cookies";
	public static final String USER_AGENT = "user_agent";
	public static final String ELAPSED_TIME = "elapsed_time";
	public static final String SIZE = "size";
	public static final String STATUS_CODE = "status_code";
	public static final String URL = "url";
	public static final String HTTP_METHOD = "http_method";
	public static final String HTTP_PROTOCOL_VERSION = "http_protocol_version";
	public static final String TIMESTAMP = "timestamp";
	public static final String TIME_YEAR = "time_year";
	public static final String TIME_MONTH = "time_month";
	public static final String TIME_DAY = "time_day";
	public static final String TIME_HOUR = "time_hour";
	public static final String TIME_MINUTE = "time_minute";
	public static final String TIME_SECOND = "time_second";
	public static final String REMOTE_USER = "remote_user";
	public static final String REMOTE_LOGNAME = "remote_logname";
	public static final String REMOTE_HOSTNAME = "remote_hostname";
	public static final String USERNAME = "username";
	
	public static final String SITESERVER = "SITESERVER";
	public static final String JSESSIONID = "JSESSIONID";
	public static final String ERROR = "error";
	public static final String ZERO = "0";

	private static final String USERNAME_REGEX = "username=(.*)&";

	private static final Logger LOG = LoggerFactory.getLogger(LogParser.class);
	
	private static final SimpleDateFormat logDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
	private static final Pattern patter = Pattern.compile(USERNAME_REGEX);

	private void parse ( Reader reader ) throws IOException, ParseException, Exception {
		BufferedReader in = new BufferedReader(reader);
		long lineCount = 0;
		try {
			String line = null;
			while ( ( line = in.readLine() ) != null ) {
				lineCount++;				
				Map<String,String> logLine = parseLine(line);
				if (logLine.get("SITESERVER")!= null)
					System.out.println (logLine.get("SITESERVER"));
			}
		} finally {
			in.close();
		}
		LOG.info("finished parsing file. Total number of log lines:" + lineCount);
	}

	public Map<String,String> parseLine ( String line ) throws ParseException {
		HashMap<String, String> logLine = new HashMap<String, String>();
	
		try {
			StringTokenizer tokenizer = new StringTokenizer ( line );
			
			Matcher matcher = patter.matcher(line);
			if ( line.contains("username") ) {
				if ( matcher.find() ) {
					logLine.put(USERNAME, matcher.group(1));;
				}
			}

			logLine.put(ERROR, "false"); // used to signal error conditions
			logLine.put(REMOTE_HOSTNAME, tokenizer.nextToken());
			logLine.put(REMOTE_LOGNAME, normalize(tokenizer.nextToken()));
			logLine.put(REMOTE_USER, normalize(tokenizer.nextToken()));
			Date date = logDateFormat.parse(tokenizer.nextToken("]").substring(2));
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			logLine.put(TIMESTAMP, Long.toString(date.getTime()));
			logLine.put(TIME_YEAR, Integer.toString(calendar.get(Calendar.YEAR)));
			logLine.put(TIME_MONTH, Utils.zeroPadding(Integer.toString(calendar.get(Calendar.MONTH) + 1),2));
			logLine.put(TIME_DAY, Utils.zeroPadding(Integer.toString(calendar.get(Calendar.DAY_OF_MONTH)),2));
			logLine.put(TIME_HOUR, Utils.zeroPadding(Integer.toString(calendar.get(Calendar.HOUR_OF_DAY)),2));
			logLine.put(TIME_MINUTE, Utils.zeroPadding(Integer.toString(calendar.get(Calendar.MINUTE)),2));
			logLine.put(TIME_SECOND, Utils.zeroPadding(Integer.toString(calendar.get(Calendar.SECOND)),2));

			tokenizer.nextToken("\"");
			String request = tokenizer.nextToken("\"");
			String[] requestParts = request.split(" ");
			logLine.put(HTTP_METHOD, requestParts[0]);
			logLine.put(URL, requestParts[1]);
			logLine.put(HTTP_PROTOCOL_VERSION, normalize(requestParts[2]));

			tokenizer.nextToken(" ");
			logLine.put(STATUS_CODE, tokenizer.nextToken());
			logLine.put(SIZE, normalize(tokenizer.nextToken(), ZERO)); // in bytes
			logLine.put(ELAPSED_TIME, tokenizer.nextToken()); // in microseconds
			
			tokenizer.nextToken("\"");
			logLine.put(USER_AGENT, tokenizer.nextToken("\""));

			tokenizer.nextToken(" ");
			String cookies = tokenizer.nextToken(">").substring(2);
			logLine.put(COOKIES, normalize(cookies));
			logLine.putAll(parseCookies(cookies));
			
			tokenizer.nextToken(" ");
			cookies = tokenizer.nextToken(">").substring(2);
			logLine.put(SET_COOKIES, normalize(cookies));
			logLine.putAll(parseCookies(cookies));
			
			tokenizer.nextToken("\"");
			logLine.put(REFERER, normalize(tokenizer.nextToken("\"")));
			
			tokenizer.nextToken(" ");
			logLine.put(PORT, tokenizer.nextToken());
		} catch ( Exception e ) {
			LOG.debug ( "{}: {} parsing {}", e.getClass().getSimpleName(), e.getMessage(), line );
			logLine.put(ERROR, "true"); // used to signal error conditions
		}

		return logLine;
	}

	private String normalize ( String str ) {
		return ( "-".equals(str) ) ? null : str;
	}
	
	private String normalize ( String str, String value ) {
		return ( "-".equals(str) ) ? value : str;
	}
	
	private Map<String,String> parseCookies ( String cookies ) throws ParseException {
		HashMap<String, String> parsedCookies = new HashMap<String, String>();
		if ( !"-".equals(cookies) ) {
			StringTokenizer tokenizer = new StringTokenizer ( cookies );
			while ( tokenizer.hasMoreTokens() ) {
				parsedCookies.put(tokenizer.nextToken("=").trim(), tokenizer.nextToken(";").trim().substring(1));
				if ( tokenizer.hasMoreTokens() ) tokenizer.nextToken(" ");
			}
		}
		return parsedCookies;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("");
			System.exit(1);
		}

		LogParser manager = new LogParser();
		String input = args[0];
		LOG.info("Parsing file:" + input);
		Reader reader = (input.endsWith(".gz")) ? new InputStreamReader( new GZIPInputStream( new FileInputStream(input) ) ) :  new FileReader(input);
		manager.parse(reader);
	}

}
