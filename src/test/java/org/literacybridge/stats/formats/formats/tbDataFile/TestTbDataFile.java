package org.literacybridge.stats.formats.formats.tbDataFile;

import junit.framework.TestCase;
import org.apache.commons.lang.WordUtils;
import org.h2.util.StringUtils;
import org.junit.Test;
import org.literacybridge.dashboard.dbTables.TbDataLine;
import org.literacybridge.stats.formats.tbData.TbDataParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TestTbDataFile {
    String[][] columnMap = {
        {"PROJECT", "project"},
        {"UPDATE_DATE_TIME", "update_date_time"},
        {"OUT_SYNC_DIR", "out_synch_dir"},
        {"LOCATION", "location"},
        {"ACTION", "action"},
        {"DURATION_SEC", "duration_sec"},
        {"OUT-SN", "out_sn"},
        {"OUT-DEPLOYMENT", "out_deployment"},
        {"OUT-IMAGE", "out_package"},
        {"OUT-FW-REV", "out_firmware"},
        {"OUT-COMMUNITY", "out_community"},
        {"OUT-ROTATION-DATE", "out_rotation"},
        {"IN-SN", "in_sn"},
        {"IN-DEPLOYMENT", "in_deployment"},
        {"IN-IMAGE", "in_package"},
        {"IN-FW-REV", "in_firmware"},
        {"IN-COMMUNITY", "in_community"},
        {"IN-LAST-UPDATED", "in_update_timestamp"},
        {"IN-SYNC-DIR", "in_synchdir"},
        {"IN-DISK-LABEL", "in_disk_label"},
        {"CORRUPTION?", "disk_corrupted"},
        {"FLASH-SN", "flash_sn"},
        {"FLASH-REFLASHES", "flash_reflashes"},
        {"FLASH-DEPLOYMENT", "flash_deployment"},
        {"FLASH-IMAGE", "flash_package"},
        {"FLASH-COMMUNITY", "flash_community"},
        {"FLASH-LAST-UPDATED", "flash_last_updated"},
        {"FLASH-CUM-DAYS", "flash_cumulative_days"},
        {"FLASH-CORRUPTION-DAY", "flash_corruption_day"},
        {"FLASH-VOLT", "flash_last_initial_v"},
        {"FLASH-POWERUPS", "flash_powerups"},
        {"FLASH-PERIODS", "flash_periods"},
        {"FLASH-ROTATIONS", "flash_rotations"},
        {"FLASH-MSGS", "flash_num_messages"},
        {"FLASH-STARTS", "flash_started"},
        {"FLASH-APPLIED", "flash_applied"},
        {"FLASH-USELESS", "flash_useless"},
        {"FLASH-PERIOD-R0", "flash_period_0"},
        {"FLASH-HRS-POST-UPDATE-R0", "flash_hours_post_update_0"},
        {"FLASH-VOLT-R0", "flash_initial_v_0"},
    };

    @Test
    public void testStatsFileMultiImage() throws IOException, IllegalAccessException, InvocationTargetException {
        testStatsFile("2022y05m31d-0073");
    }

    @Test
    public void testStatsFileSingleImage() throws IOException, IllegalAccessException, InvocationTargetException {
        testStatsFile("2022y06m16d-005d");
    }

    @SuppressWarnings("deprecation")
    public void testStatsFile(String fileId) throws IOException, IllegalAccessException, InvocationTargetException {
        // Parse the tbdata-v3-.csv file with the TbDataParser.
        String fn = String.format("src/test/resources/tbDataFiles/tbData-v03-%s.csv", fileId);
        File tbDataFile = new File(fn);
        TbDataParser parser = new TbDataParser();
        List<TbDataLine> lines = parser.parseTbDataFile(tbDataFile, true);
        List<Map<String, String>> tbDataMap = new ArrayList<>();

        // Extract the data into something more usable.
        for (TbDataLine line : lines) {
            Map<String, String> tbLineMap = new LinkedHashMap<>();
            // Extract only the ones we care about
            for (String[] fieldNames : columnMap) {
                String fieldName = fieldNames[0];
                Method getter = getter(fieldName);
                if (getter != null) {
                    Object o = getter.invoke(line);
                    if (o != null) {
                        tbLineMap.put(fieldName, o.toString());
                    }
                }
            }
            tbDataMap.add(tbLineMap);
        }

        // Parse the tbdata-.log file
        List<Map<String, String>> tbLogMap = new ArrayList<>();
        fn = String.format("src/test/resources/tbDataFiles/tbData-%s.log", fileId);
        File tbDataLog = new File(fn);
        try (FileInputStream fis = new FileInputStream(tbDataLog);
             InputStreamReader isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
             BufferedReader tbDataBr = new BufferedReader(isr)) {
            String dataLine;
            while ((dataLine = tbDataBr.readLine()) != null) {
                String[] fields = dataLine.split(",");
                Map<String, String> tbLogLineMap = new LinkedHashMap<>();
                for (int ix=2; ix<fields.length; ix++) {
                    String[] parts = fields[ix].split(":", 2);
                    tbLogLineMap.put(parts[0], parts[1]);
                }
                tbLogMap.add(tbLogLineMap);
            }
        }

        // Compare the two.
        int comparedLines = 0;
        int comparedFields = 0;
        for (int i = 0; i < tbLogMap.size(); i++) {
            comparedLines += 1;
            // Two parallel items to compare.
            Map<String, String> csvLine = tbDataMap.get(i);
            Map<String, String> logLine = tbLogMap.get(i);
            for (String[] keys : columnMap) {
                String csvItem = csvLine.get(keys[0]);
                String logItem = logLine.get(keys[1]);
                // Only check items where where we have both.
                if (csvItem==null || logItem==null) continue;
                comparedFields += 1;
                if (!StringUtils.equals(csvItem, logItem)) {
                    // Maybe this is one of the dates?
                    try {
                        String csvDate = new Date(csvItem).toString();
                        String logDate = new Date(logItem).toString();
                        if (csvDate.equals(logDate))
                            continue;
                    } catch (Exception ignored) {}
                    // In the log file, commas are replaced with semicolons (commas delimit the fields. Try munging
                    // the csv value to see if that makes a match.
                    if (csvItem != null) csvItem = csvItem.replace(',', ';');
                    TestCase.assertTrue(String.format("Expected to match %s: %s <-> %s: %s\n", keys[0], csvItem, keys[1], logItem),
                        StringUtils.equals(csvItem, logItem));
                }
            }
        }

        System.out.printf("%d lines from csv, %d lines from log, compared %d lines, %d fields\n", tbDataMap.size(), tbLogMap.size(), comparedLines, comparedFields);
    }

    private Method getter(String fieldName) {
        Method[] methods = TbDataLine.class.getMethods();
        char[] FIELD_DELIMITERS = new char[]{'-'};
        String[] fieldNameParts = fieldName.toLowerCase().split("[? _-]");

        StringBuilder nameBuilder = new StringBuilder("get");
        for (String propertyNamePart : fieldNameParts) {
            nameBuilder.append(WordUtils.capitalize(propertyNamePart, FIELD_DELIMITERS));
        }
        String geterName = nameBuilder.toString();
        for (Method method : methods) {
            if (method.getName().equals(geterName)) {
                return method;
            }
        }
        return null;
    }

}
