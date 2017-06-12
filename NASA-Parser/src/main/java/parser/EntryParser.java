package parser;

import com.google.common.base.Splitter;
import model.DBEntry;
import model.ImmutableDBEntry;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This calls is built specifically to parse out information from Nasa data files
 * Created by awaldman on 4/18/17.
 */
public class EntryParser {

    private static final Pattern p = Pattern.compile("(.+) - - \\[(.+)\\] \"(.*?) (.+?)\" (\\d{3}) (-|\\d*)");
    private static final Pattern usernameP = Pattern.compile("^\\/~(.*?)(\\/.*)$");
    private static final DateTimeFormatter dateTime = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
    private static final Pattern fileEWithUser = Pattern.compile("^\\/~(.*)\\/(.*)\\.(.*)$");
    private static final Pattern fileEWithoutUser = Pattern.compile("^\\/(.*)\\.(.*) (.*)$");

    /**
     * Due to the data being sent as bytes (due to serialisation) changing it to a string
     * and then splitting would make it easier to then manipulate
     * @param entryString
     * @return DBEntry
     */
    public static List<DBEntry> parse(String entryString) {
        List<DBEntry> dbEntries = new ArrayList<>();
        List<String> entries = Splitter
            .on("\n")
            .trimResults()
            .omitEmptyStrings()
            .splitToList(entryString);
        for (String entry : entries) {
            MatchResult matchResult = patternMatching(entry);
            if (matchResult == null) {
                System.out.print("Null match " + "   " + entry);
                continue;
            }
            String entryID = createHashEntryValue(entry);
            String client = matchResult.group(1);
            String username = getUsername(matchResult);
            DateTime timeAndZone = makeDateTime(matchResult);
            String fileExtension = getFileExtension(matchResult);
            String restApiCall = matchResult.group(3);
            Integer responseCode = Integer.parseInt(matchResult.group(5));
            Integer payloadSize = getPayloadSize(matchResult.group(6));
            String resource = getResource(matchResult);
            dbEntries.add(ImmutableDBEntry
                .builder()
                    .entryID(entryID)
                    .client(client)
                    .username(username)
                    .datetime(timeAndZone)
                    .fileExtension(fileExtension)
                    .restfulAPI(restApiCall)
                    .responseCode(responseCode)
                    .payloadSize(payloadSize)
                    .resourceAccessed(resource)
                .build()
            );
        }
        return dbEntries;
    }

    /**
     * This method creates a MD5 hash from the entry string to enter into the db
     * this is to make sure there is no duplicate data
     * in this case there wont (NASA Data) be however good practise
     * @param singleEntry entry used for hash ID
     * @return stringBuilder.toString | null
     */
    @Nullable
    private static String createHashEntryValue(String singleEntry) {
        try {
            // message digests are one way hash function
            MessageDigest md = MessageDigest.getInstance("MD5");
            // updates the digest with set of bytes
            md.update(singleEntry.getBytes());
            // perform the hash calculation by digesting and return array of bytes
            byte[] byteData = md.digest();
            StringBuilder stringBuilder = new StringBuilder(byteData.length);
            for (byte aByteData : byteData) {
                // %02x means number of digits is less than 2 pad to the left and x means hex value
                stringBuilder.append(String.format("%02x", aByteData));
            }
            return stringBuilder.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * This class will find the file extension of the resource access
     * If there is not one then it will also return that there is not one
     * @param fileExtension give the matched entry
     * @return fileExtension | null
     */
    @Nullable
    private static String getFileExtension(MatchResult fileExtension) {
        //1 with username
        Matcher withUser = fileEWithUser.matcher(fileExtension.group(4));
        if (withUser.matches()) {
            return withUser.group(3);
        }
        //3 without username  /images/logo.gif HTTP/1.0
        Matcher withoutUser = fileEWithoutUser.matcher(fileExtension.group(4));
        if (withoutUser.matches()) {
            return withoutUser.group(2);
        }

        return null;
    }

    /**
     * This class will find the file extension of the resource access
     * If there is not one then it will also return that there is not one
     * with string manipulation
     * @param fileExtension give the matched entry
     * @return fileExtension | null
     */
    @Nullable
    private static String getFileExtensionS(MatchResult fileExtension) {
        //get the file extension if it exists, if not return default
        String[] strings = fileExtension.group(4).split("\\s+");
        String[] strings1 = strings[0].split("\\.");
        if (strings1.length >= 2) {
            return strings1[1];
        }

        // return null as default, being there no resource
        return null;
    }

    /**
     * This method will return the resource location
     * @param groupFour give the matched entry
     * @return resource | /
     */
    private static String getResource(MatchResult groupFour){
        Matcher m = usernameP.matcher(groupFour.group(4));
        if (m.matches()) {
            return m.group(2);
        }

        String[] resource = groupFour.group(4).split("\\s+");
        if (resource.length > 1) {
            return resource[0];
        }

        // default for root resource
        return "/";
    }

    /**
     * This method will return the resource location
     * using string manipulation
     * @param groupFour give the matched entry
     * @return resource | /
     */
    private static String getResourceS(MatchResult groupFour) {
        // resource with username
            String[] strings = groupFour.group(4).split("\\s+");
            if (strings[0].contains("/~")) {
                // username exists on containing the characters above
                String[] strings1 = strings[0].split("\\/");
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 2; i < strings1.length; i++) {
                    stringBuilder.append("/");
                    stringBuilder.append(strings1[i]);
                }
                return stringBuilder.toString();
            }

        // resource with no username
        if (strings.length > 1) {
            return strings[0];
        }

        // default for root resource
        return "/";
    }

    /**
     * This method will return the user name if it exists
     * @param groupFour give the matched entry
     * @return username | null
     */
    @Nullable
    private static String getUsername(MatchResult groupFour) {
        Matcher m = usernameP.matcher(groupFour.group(4));
        if (m.matches()) {
            return m.group(1);
        }
        return null;
    }

    /**
     * This method will return the user name if it exists
     * this one however uses string manipulation
     * @param groupFour give the matched entry
     * @return username | null
     */
    @Nullable
    private static String getUsernameS(MatchResult groupFour) {
        // check to see if entry has username
        if (groupFour.group(4).contains("/~")) {
            // username exists on containing the characters above
            String[] strings = groupFour.group(4).split("\\/~");
            String[] strings1 = strings[1].split("\\/");
            return strings1[0];
        }
        // no username return null
        return null;
    }

    /**
     * This method will get the payload size
     * @param groupSix give the matched entry for group 6 of the match
     * @return Integer return the payload size
     */
    private static Integer getPayloadSize(String groupSix) {
        if (groupSix.equals("-")){
            return 0;
        } else {
            return Integer.parseInt(groupSix);
        }
    }

    /**
     * This method will get the date and return the data in a nice format
     * including changing it to central timezone
     * @param dateTimeZone give the matched entry
     * @return DateTime | null
     */
    @Nullable
    private static DateTime makeDateTime(MatchResult dateTimeZone) {
        try {
            return dateTime.parseDateTime(dateTimeZone.group(2)).withZone(DateTimeZone.UTC);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * This method returns if the entry matches the data pattern needed to be entered into the DB
     * @param entry give the entry to match and be parsed
     * @return MatchResult | null
     */
    @Nullable
    private static MatchResult patternMatching(String entry) {
        Matcher m = p.matcher(entry);
        boolean b = m.matches();
        if (b) {
            return m;
        }
        return null;
    }
}