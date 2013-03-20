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

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestLogParser {
	
	private void assertKeys ( Map<String,String> m, String[] keys, boolean check ) {
		for (String key : keys) {
			assertEquals (check, m.containsKey(key));
		}
	}
	
    @Test
    public void parseLine_01() throws ParseException {
    	LogParser parser = new LogParser();
    	Map<String,String> m = parser.parseLine("1.22.333.444 - - [25/Feb/2013:01:02:03 +0100] \"GET /img/favicon.ico HTTP/1.1\" 200 921 907 \"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)\" <SITESERVER=ID=0000000000000000000123> <-> \"-\" 80");
    	
    	assertNotNull(m);
    	assertEquals("1.22.333.444", m.get(LogParser.REMOTE_HOSTNAME));
    	assertEquals(null, m.get(LogParser.REMOTE_LOGNAME));
    	assertEquals(null, m.get(LogParser.REMOTE_USER));
    	assertEquals("2013", m.get(LogParser.TIME_YEAR));
    	assertEquals("02", m.get(LogParser.TIME_MONTH));
    	assertEquals("25", m.get(LogParser.TIME_DAY));
    	assertEquals("00", m.get(LogParser.TIME_HOUR)); // timezone is +0100
    	assertEquals("02", m.get(LogParser.TIME_MINUTE));
    	assertEquals("03", m.get(LogParser.TIME_SECOND));
    	assertEquals("1361750523000", m.get(LogParser.TIMESTAMP));
    	assertEquals("GET", m.get(LogParser.HTTP_METHOD));
    	assertEquals("/img/favicon.ico", m.get(LogParser.URL));
    	assertEquals("HTTP/1.1", m.get(LogParser.HTTP_PROTOCOL_VERSION));
    	assertEquals("200", m.get(LogParser.STATUS_CODE));
    	assertEquals("921", m.get(LogParser.SIZE));
    	assertEquals("907", m.get(LogParser.ELAPSED_TIME));
    	assertEquals("Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)", m.get(LogParser.USER_AGENT));
    	assertEquals("SITESERVER=ID=0000000000000000000123", m.get(LogParser.COOKIES));
    	assertEquals("ID=0000000000000000000123", m.get(LogParser.SITESERVER));
    	assertEquals(null, m.get(LogParser.SET_COOKIES));
    	assertEquals(null, m.get(LogParser.REFERER));
    	assertEquals("80", m.get(LogParser.PORT));

    	assertKeys(m, new String[]{LogParser.REMOTE_LOGNAME, LogParser.REMOTE_USER, LogParser.SET_COOKIES, LogParser.REFERER}, true);
    	assertKeys(m, new String[]{"foo"}, false);
    	
    	assertEquals("false", m.get(LogParser.ERROR));
    }
    
    @Test
    public void parseLine_02() throws ParseException {
    	LogParser parser = new LogParser();
    	Map<String,String> m = parser.parseLine("1.22.333.444 - - [24/Feb/2013:23:59:38 +0100] \"GET /jsp/user.do?XXXX");
    	
    	assertNotNull(m);
    	assertEquals("1.22.333.444", m.get(LogParser.REMOTE_HOSTNAME));
    	assertEquals(null, m.get(LogParser.REMOTE_LOGNAME));
    	assertEquals(null, m.get(LogParser.REMOTE_USER));
    	assertEquals("2013", m.get(LogParser.TIME_YEAR));
    	assertEquals("02", m.get(LogParser.TIME_MONTH));
    	assertEquals("24", m.get(LogParser.TIME_DAY));
    	assertEquals("22", m.get(LogParser.TIME_HOUR)); // timezone is +0100
    	assertEquals("59", m.get(LogParser.TIME_MINUTE));
    	assertEquals("38", m.get(LogParser.TIME_SECOND));
    	assertEquals("1361746778000", m.get(LogParser.TIMESTAMP));
    	assertEquals("GET", m.get(LogParser.HTTP_METHOD));
    	assertEquals("/jsp/user.do?XXXX", m.get(LogParser.URL));
    	assertEquals(null, m.get(LogParser.HTTP_PROTOCOL_VERSION));
    	assertEquals(null, m.get(LogParser.STATUS_CODE));
    	assertEquals(null, m.get(LogParser.SIZE));
    	assertEquals(null, m.get(LogParser.ELAPSED_TIME));
    	assertEquals(null, m.get(LogParser.USER_AGENT));
    	assertEquals(null, m.get(LogParser.COOKIES));
    	assertEquals(null, m.get(LogParser.SITESERVER));
    	assertEquals(null, m.get(LogParser.SET_COOKIES));
    	assertEquals(null, m.get(LogParser.REFERER));
    	assertEquals(null, m.get(LogParser.PORT));

    	assertKeys(m, new String[]{LogParser.REMOTE_LOGNAME, LogParser.REMOTE_USER}, true);
    	assertKeys(m, new String[]{LogParser.HTTP_PROTOCOL_VERSION, LogParser.STATUS_CODE, LogParser.SIZE, 
    			LogParser.ELAPSED_TIME, LogParser.USER_AGENT, LogParser.COOKIES, LogParser.SITESERVER, 
    			LogParser.SET_COOKIES, LogParser.REFERER, LogParser.PORT}, false);
    	
    	assertEquals("true", m.get(LogParser.ERROR));
    }

    @Test
    public void parseLine_03() throws ParseException {
    	LogParser parser = new LogParser();
    	Map<String,String> m = parser.parseLine("1.22.333.444 - - [25/Feb/2013:00:00:00 +0100] \"GET /jsp/foo.do?bar=123 HTTP/1.1\" 200 256 123456 \"Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.9.2.17) Gecko/20110420\" <SITESERVER=ID=0000000000000000000123; JSESSIONID=adhfafdua134913241324; foo=bar;> <-> \"http://www.example.com/from/here\" 80");
    	
    	assertNotNull(m);
    	assertEquals("1.22.333.444", m.get(LogParser.REMOTE_HOSTNAME));
    	assertEquals(null, m.get(LogParser.REMOTE_LOGNAME));
    	assertEquals(null, m.get(LogParser.REMOTE_USER));
    	assertEquals("2013", m.get(LogParser.TIME_YEAR));
    	assertEquals("02", m.get(LogParser.TIME_MONTH));
    	assertEquals("24", m.get(LogParser.TIME_DAY));
    	assertEquals("23", m.get(LogParser.TIME_HOUR)); // timezone is +0100
    	assertEquals("00", m.get(LogParser.TIME_MINUTE));
    	assertEquals("00", m.get(LogParser.TIME_SECOND));
    	assertEquals("1361746800000", m.get(LogParser.TIMESTAMP));
    	assertEquals("GET", m.get(LogParser.HTTP_METHOD));
    	assertEquals("/jsp/foo.do?bar=123", m.get(LogParser.URL));
    	assertEquals("HTTP/1.1", m.get(LogParser.HTTP_PROTOCOL_VERSION));
    	assertEquals("200", m.get(LogParser.STATUS_CODE));
    	assertEquals("256", m.get(LogParser.SIZE));
    	assertEquals("123456", m.get(LogParser.ELAPSED_TIME));
    	assertEquals("Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.9.2.17) Gecko/20110420", m.get(LogParser.USER_AGENT));
    	assertEquals("SITESERVER=ID=0000000000000000000123; JSESSIONID=adhfafdua134913241324; foo=bar;", m.get(LogParser.COOKIES));
    	assertEquals("ID=0000000000000000000123", m.get(LogParser.SITESERVER));
    	assertEquals("adhfafdua134913241324", m.get(LogParser.JSESSIONID));
    	assertEquals(null, m.get(LogParser.SET_COOKIES));
    	assertEquals("http://www.example.com/from/here", m.get(LogParser.REFERER));
    	assertEquals("80", m.get(LogParser.PORT));

    	assertEquals("false", m.get(LogParser.ERROR));
    }

    @Test
    public void parseLine_04() throws ParseException {
    	LogParser parser = new LogParser();
    	Map<String,String> m = parser.parseLine("1.22.333.444 foo bar [25/Feb/2013:00:00:00 +0100] \"POST /jsp/login.do HTTP/1.1\" 302 - 123456 \"Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.0.3) Gecko/20060426 Firefox/1.5.0.3\" <-> <SITESERVER=ID=0000000000000000000123> \"-\" 80");
    	
    	assertNotNull(m);
    	assertEquals("1.22.333.444", m.get(LogParser.REMOTE_HOSTNAME));
    	assertEquals("foo", m.get(LogParser.REMOTE_LOGNAME));
    	assertEquals("bar", m.get(LogParser.REMOTE_USER));
    	assertEquals("2013", m.get(LogParser.TIME_YEAR));
    	assertEquals("02", m.get(LogParser.TIME_MONTH));
    	assertEquals("24", m.get(LogParser.TIME_DAY));
    	assertEquals("23", m.get(LogParser.TIME_HOUR)); // timezone is +0100
    	assertEquals("00", m.get(LogParser.TIME_MINUTE));
    	assertEquals("00", m.get(LogParser.TIME_SECOND));
    	assertEquals("1361746800000", m.get(LogParser.TIMESTAMP));
    	assertEquals("POST", m.get(LogParser.HTTP_METHOD));
    	assertEquals("/jsp/login.do", m.get(LogParser.URL));
    	assertEquals("HTTP/1.1", m.get(LogParser.HTTP_PROTOCOL_VERSION));
    	assertEquals("302", m.get(LogParser.STATUS_CODE));
    	assertEquals("0", m.get(LogParser.SIZE));
    	assertEquals("123456", m.get(LogParser.ELAPSED_TIME));
    	assertEquals("Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.0.3) Gecko/20060426 Firefox/1.5.0.3", m.get(LogParser.USER_AGENT));
    	assertEquals(null, m.get(LogParser.COOKIES));
    	assertEquals("ID=0000000000000000000123", m.get(LogParser.SITESERVER));
    	assertEquals(null, m.get(LogParser.JSESSIONID));
    	assertEquals("SITESERVER=ID=0000000000000000000123", m.get(LogParser.SET_COOKIES));
    	assertEquals(null, m.get(LogParser.REFERER));
    	assertEquals("80", m.get(LogParser.PORT));

    	assertEquals("false", m.get(LogParser.ERROR));
    }
    
}
