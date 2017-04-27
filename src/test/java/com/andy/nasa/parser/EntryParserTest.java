package com.andy.nasa.parser;

import com.andy.nasa.model.DBEntry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the test class for the parser class making sure it can parse the data as expected.
 * Created by awaldman on 4/24/17.
 */
public class EntryParserTest {

	private List<String> entries;

	@BeforeClass
	public void setUpListOfEntries() {
		entries = new ArrayList<>();
		entries.add("202.32.92.47 - - [01/Jun/1995:00:00:59 -0600] \"GET /~scottp/publish.html\" 200 271");
		entries.add("129.217.223.5 - - [01/Jun/1995:09:10:10 -0600] \"GET /~macphed/finite/fe_resources/node53.html\" 200 1864");
		entries.add("soa006.usask.ca - - [01/Jun/1995:09:17:44 -0600] \"GET /images/logo.gif HTTP/1.0\" 200 2273");
		entries.add("ccchong.net6a.io.org - - [01/Jun/1995:09:19:33 -0600] \"GET / HTTP/1.0\" 200 3384");
		entries.add("eng23.usask.ca - - [23/Dec/1995:18:05:39 -0600] \"GET / HTTP/1.0\" 200 3856");
		entries.add("sdsdvsdvsdvdsv");
	}

	@Test
	public void testPatternMatching() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		Assert.assertFalse(dbEntryOne.isEmpty());
	}

	@Test
	public void testMultiPatternMatching() {
		String multiParse = String.join("\n", entries);
		List<DBEntry> dbEntryAll = EntryParser.parse(multiParse);
		Assert.assertEquals(dbEntryAll.size(), 5);
	}

	@Test
	public void testNonMatchingEntry() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(5));
		Assert.assertTrue(dbEntryOne.isEmpty());
	}

	@Test
	public void testCreateHash() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(1));
		List<DBEntry> dbEntryThree = EntryParser.parse(entries.get(2));
		List<DBEntry> dbEntryFour = EntryParser.parse(entries.get(3));
		List<DBEntry> dbEntryFive = EntryParser.parse(entries.get(4));

		Assert.assertEquals(dbEntryOne.get(0).entryID(), "adfa010222f0f1fd1ccd0d9b502bf77e");
		Assert.assertEquals(dbEntryTwo.get(0).entryID(), "77072cf14c83fa7012a66b4e440f6590");
		Assert.assertEquals(dbEntryThree.get(0).entryID(), "5af83098545e7608003a03cd47a0c456");
		Assert.assertEquals(dbEntryFour.get(0).entryID(), "d1eccc4752ff761070f69cb44300370c");
		Assert.assertEquals(dbEntryFive.get(0).entryID(), "1c04f2bd89e3dbd6a6ff2bd46bfcd353");
	}

	@Test
	public void testGetFileExtension() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(2));
		List<DBEntry> dbEntryThree = EntryParser.parse(entries.get(3));
		List<DBEntry> dbEntryFour = EntryParser.parse(entries.get(4));

		Assert.assertEquals(dbEntryOne.get(0).fileExtension(), "html");
		Assert.assertEquals(dbEntryTwo.get(0).fileExtension(), "gif");
		Assert.assertNull(dbEntryThree.get(0).fileExtension());
		Assert.assertNull(dbEntryFour.get(0).fileExtension());
	}

	@Test
	public void testGetClient() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(1));
		List<DBEntry> dbEntryThree = EntryParser.parse(entries.get(2));
		List<DBEntry> dbEntryFour = EntryParser.parse(entries.get(3));
		List<DBEntry> dbEntryFive = EntryParser.parse(entries.get(4));

		Assert.assertEquals(dbEntryOne.get(0).client(), "202.32.92.47");
		Assert.assertEquals(dbEntryTwo.get(0).client(), "129.217.223.5");
		Assert.assertEquals(dbEntryThree.get(0).client(), "soa006.usask.ca");
		Assert.assertEquals(dbEntryFour.get(0).client(), "ccchong.net6a.io.org");
		Assert.assertEquals(dbEntryFive.get(0).client(), "eng23.usask.ca");
	}

	@Test
	public void testGetUsername() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(1));

		Assert.assertEquals(dbEntryOne.get(0).username(), "scottp");
		Assert.assertEquals(dbEntryTwo.get(0).username(), "macphed");
	}

	@Test
	public void testNoUserName() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(2));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(3));
		List<DBEntry> dbEntryThree = EntryParser.parse(entries.get(4));


		Assert.assertNull(dbEntryOne.get(0).username());
		Assert.assertNull(dbEntryTwo.get(0).username());
		Assert.assertNull(dbEntryThree.get(0).username());
	}

	@Test
	public void testGetTimezoneDate() {
		DateTimeFormatter dateTime = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		DateTime dt = dateTime.parseDateTime("01/Jun/1995:00:00:59 -0600").withZone(DateTimeZone.UTC);
		Assert.assertEquals(dbEntryOne.get(0).datetime(), dt);
	}

	@Test
	public void testGetRestAPICall() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(1));
		List<DBEntry> dbEntryThree = EntryParser.parse(entries.get(2));
		List<DBEntry> dbEntryFour = EntryParser.parse(entries.get(3));
		List<DBEntry> dbEntryFive = EntryParser.parse(entries.get(4));

		Assert.assertEquals(dbEntryOne.get(0).restfulAPI(), "GET");
		Assert.assertEquals(dbEntryTwo.get(0).restfulAPI(), "GET");
		Assert.assertEquals(dbEntryThree.get(0).restfulAPI(), "GET");
		Assert.assertEquals(dbEntryFour.get(0).restfulAPI(), "GET");
		Assert.assertEquals(dbEntryFive.get(0).restfulAPI(), "GET");
	}

	@Test
	public void testGetResponseCode() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(1));
		List<DBEntry> dbEntryThree = EntryParser.parse(entries.get(2));
		List<DBEntry> dbEntryFour = EntryParser.parse(entries.get(3));
		List<DBEntry> dbEntryFive = EntryParser.parse(entries.get(4));

		Assert.assertEquals(dbEntryOne.get(0).responseCode(), Integer.valueOf(200));
		Assert.assertEquals(dbEntryTwo.get(0).responseCode(), Integer.valueOf(200));
		Assert.assertEquals(dbEntryThree.get(0).responseCode(), Integer.valueOf(200));
		Assert.assertEquals(dbEntryFour.get(0).responseCode(), Integer.valueOf(200));
		Assert.assertEquals(dbEntryFive.get(0).responseCode(), Integer.valueOf(200));
	}

	@Test
	public void testGetPayloadSize() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(1));
		List<DBEntry> dbEntryThree = EntryParser.parse(entries.get(2));
		List<DBEntry> dbEntryFour = EntryParser.parse(entries.get(3));
		List<DBEntry> dbEntryFive = EntryParser.parse(entries.get(4));

		Assert.assertEquals(dbEntryOne.get(0).payloadSize(), Integer.valueOf(271));
		Assert.assertEquals(dbEntryTwo.get(0).payloadSize(), Integer.valueOf(1864));
		Assert.assertEquals(dbEntryThree.get(0).payloadSize(), Integer.valueOf(2273));
		Assert.assertEquals(dbEntryFour.get(0).payloadSize(), Integer.valueOf(3384));
		Assert.assertEquals(dbEntryFive.get(0).payloadSize(), Integer.valueOf(3856));
	}

	@Test
	public void testGetResource() {
		List<DBEntry> dbEntryOne = EntryParser.parse(entries.get(0));
		List<DBEntry> dbEntryTwo = EntryParser.parse(entries.get(1));
		List<DBEntry> dbEntryThree = EntryParser.parse(entries.get(2));
		List<DBEntry> dbEntryFour = EntryParser.parse(entries.get(3));

		Assert.assertEquals(dbEntryOne.get(0).resourceAccessed(), "/publish.html");
		Assert.assertEquals(dbEntryTwo.get(0).resourceAccessed(), "/finite/fe_resources/node53.html");

		Assert.assertEquals(dbEntryThree.get(0).resourceAccessed(), "/images/logo.gif");
		Assert.assertEquals(dbEntryFour.get(0).resourceAccessed(), "/");
	}
}
