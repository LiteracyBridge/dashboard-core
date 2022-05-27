package org.literacybridge.stats.processors;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.joda.time.LocalDateTime;
import org.literacybridge.dashboard.dbTables.TbDataLine;
import org.literacybridge.dashboard.processes.ContentUsageUpdateProcess;
import org.literacybridge.stats.DirectoryIterator;
import org.literacybridge.stats.formats.tbData.TbDataParser;
import org.literacybridge.stats.model.*;
import org.literacybridge.stats.model.validation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;

/**
 */
public class ValidatingProcessor extends AbstractDirectoryProcessor {

    private static final String DEPLOY_ID_EXPECTED = "YYYY-XX formatted string with YYYYY being the year and XX being the current deployment in this year.";
    private static final String SYNC_DIR_EXPECTED = "Sync directory could not be parsed correct.  Look at https://docs.google.com/document/d/12Q0a7x15FqeZ4ys0gYy4O2MtWYrvGDUegOXwlsG9ZQY for a desciption of the appropriate formats.";
    private static final String CHECK_DISK_REFORMAT = "chkdsk-reformat.txt";

    private static final String PROGRESS_CREATING_MANIFEST = "Creating manifest for %s...";
    private static final String PROGRESS_CREATED_MANIFEST = "done.%n";

    protected static final Logger logger = LoggerFactory.getLogger(ValidatingProcessor.class);
    public final List<ValidationError> validationErrors = new ArrayList<>();

    private final TreeMap<SyncDirId, OperationalInfo> tbDataInfo = new TreeMap<>(SyncDirId.TIME_COMPARATOR);
    private final int maxTimeWindow = 10;
    private final IdentityHashMap<SyncDirId, SyncDirId> foundSyncDirs = new IdentityHashMap<>();
    private final Set<String> deviceIncorrectlyInManifest = new HashSet<>();
    private final TbDataParser tbDataParser = new TbDataParser();

    private String currOperationalDevice = null;

    public ValidatingProcessor(ContentUsageUpdateProcess.UpdateUsageContext context) {
        super(context);
    }

    @Override
    public boolean startDeviceOperationalData(@Nonnull String device) {
        super.startDeviceOperationalData(device);
        currOperationalDevice = device;
        return true;
    }

    @Override
    public void endDeviceOperationalData() {
        super.endDeviceOperationalData();
        currOperationalDevice = null;
    }

    @Override
    public void processTbDataFile(File tbdataFile, boolean includesHeaders) throws IOException {

        int lineNumber = 1;
        List<IncorrectFilePropertyValue> incorrectFilePropertyValues = new ArrayList<>();
        final List<TbDataLine> tbDataLines = tbDataParser.parseTbDataFile(tbdataFile,
                                                                          includesHeaders);

        for (TbDataLine tbDataLine : tbDataLines) {
            processLine(tbDataLine, tbdataFile, lineNumber, incorrectFilePropertyValues);
        }

        if (!incorrectFilePropertyValues.isEmpty()) {
            validationErrors.add(
                    new TbDataHasInvalidProperties(tbdataFile, incorrectFilePropertyValues));
        }
    }

    private void processLine(TbDataLine line, File tbdataFile, int lineNumber,
                             List<IncorrectFilePropertyValue> incorrectFilePropertyValues) {

        String syncDirName = line.getUpdateDateTime() + "-" + currOperationalDevice;
        /**
         * In forgotten, distant past, the data came in a different format than in 2016 and later. That
         * format is not documented anywhere (imagine that...), but I don't think we'll ever see it again.
         * In the new format, we don't need the "deployment id". However, I'm leaving the code in case we
         * ever need to re-process any of that very old data, which uses the deployment id to calculate the
         * "syncDir".
         */
        Matcher matchv2 = DirectoryIterator.SYNC_TIME_PATTERN_V2.matcher(syncDirName);
        boolean veryOldStyle = !matchv2.matches();

        String inDeployment;
        SyncDirId syncDirId;

        String inTalkingBook = line.getInSn();
        String outTalkingBook = line.getOutSn();
        if (!inTalkingBook.equalsIgnoreCase(outTalkingBook) &&
                !inTalkingBook.equalsIgnoreCase("UNKNOWN")  &&
                // We changed all B- srns to C-
                !(inTalkingBook.startsWith("B-") && outTalkingBook.startsWith("C-")) &&
                // Next line to to an awful bug that was live for about 3 months in 2016, assigning "-- to be assigned --"
                // as serial numbers.
                !inTalkingBook.equalsIgnoreCase("-- to be assigned --") ) {
            incorrectFilePropertyValues.add(
                    new IncorrectFilePropertyValue("outTalkingBook", inTalkingBook, outTalkingBook,
                                                   lineNumber));
        }

        if (veryOldStyle) {
            DeploymentId nextDeploymentId = DeploymentId.parseContentUpdate(
                    line.getOutDeployment());
            if (nextDeploymentId.year == 0) {
                incorrectFilePropertyValues.add(
                        new IncorrectFilePropertyValue("outDeploymentId", DEPLOY_ID_EXPECTED,
                                                       line.getOutDeployment(), lineNumber));
            }

            DeploymentId currDeploymentId = DeploymentId.parseContentUpdate(line.getInDeployment());
            if (currDeploymentId.year == 0 && !currDeploymentId.id.equalsIgnoreCase("UNKNOWN")) {
                incorrectFilePropertyValues.add(
                        new IncorrectFilePropertyValue("inDeploymentId", DEPLOY_ID_EXPECTED,
                                                       line.getInDeployment(), lineNumber));

                //Parse out the deployment and sync dir to make it a date.
                //Since, there is a decent amount of unknowns, make a guess in those cases. . .
                if (nextDeploymentId.year != 0) {
                    currDeploymentId = nextDeploymentId.guessPrevious();
                    logger.warn("DeploymentID is incorrect for " + tbdataFile.getPath() + ":"
                                        + lineNumber + ".  Guessing that it should be : "
                                        + currDeploymentId.id + " based on the out version.");
                } else {
                    logger.error(
                            "Unable to resolve a deployment ID for " + tbdataFile.getPath() + ":"
                                    + lineNumber);
                }
            }

            inDeployment = currDeploymentId.id;
            syncDirId = SyncDirId.parseSyncDir(currDeploymentId, syncDirName);
        } else {
            inDeployment = line.getInDeployment();
            syncDirId = SyncDirId.parseSyncDir(null, syncDirName);
        }
        LocalDateTime localDateTime = syncDirId.dateTime;
        if (localDateTime != null) {
            if (line.getAction().toLowerCase().startsWith("update") || line.getAction()
                    .equalsIgnoreCase("stats-only")) {
                OperationalInfo operationalInfo = new OperationalInfo(currOperationalDevice,
                                                                      syncDirName, localDateTime,
                                                                      inTalkingBook, outTalkingBook,
                                                                      inDeployment,
                                                                      line.getOutDeployment(),
                                                                      line.getInCommunity(),
                                                                      line.getOutCommunity());

                //Theoretically, we can have dups, but it is unlikely.  In this case, log and add at a later millisecond.
                logger.debug(
                        String.format("    operationalData for sync dir '%s', ts: %s", syncDirId,
                                      syncDirId.dateTime));
                while ((operationalInfo = tbDataInfo.put(syncDirId, operationalInfo)) != null) {
                    // This seems wrong. The key is the timestamp + tbcd id. We just replaced the previous value for that key,
                    // and are now about to add that other value with a new (& different) key.
                    syncDirId = syncDirId.addMilli();
                }
            } else {
                result.addUnexpectedOperationalAction(currRoot.getName(), currOperationalDevice, tbdataFile.getName(),
                                                      line.getAction());
            }
        } else {
            result.addCorruptOperationLine(currRoot.getName(), currOperationalDevice, tbdataFile.getName());
            logger.error("Corrupt line " + tbdataFile.getPath() + ":" + lineNumber);
            incorrectFilePropertyValues.add(
                    new IncorrectFilePropertyValue("syncDirName", SYNC_DIR_EXPECTED, syncDirName,
                                                   lineNumber));
        }
    }

    @Override
    public void processSyncDir(SyncDirId syncDirId, File syncDir) throws Exception {

        //Find closest matching TbData entry
        SyncDirId tbDataEntry = findMatchingTbDataEntry(syncDirId);
        logger.debug(String.format("    validating talkingbookData sync dir '%s': %s", syncDirId,
                                   tbDataEntry));

        //Verify each sync directory matches to EXACTLY one entry.
        LocalDateTime maxAllowableTimeV1 = syncDirId.dateTime.plusMinutes(maxTimeWindow);

        //If this file is due to major corruption, just bail out.
        File chkdiskFile = new File(syncDir, CHECK_DISK_REFORMAT);
        if (chkdiskFile.exists()) {
            SyncDirId previousSyncDir = foundSyncDirs.put(tbDataEntry, tbDataEntry);
            if (previousSyncDir != null) {
                validationErrors.add(new MultipleTbDatasMatchError(tbDataEntry.dirName,
                                                                   tbDataInfo.get(
                                                                           tbDataEntry).deviceName));
            }

            return;
        }

        if (syncDir.list().length == 0) {
            validationErrors.add(new EmptySyncDirectory(syncDir));
            return;
        }

        if (tbDataEntry == null || (manifest.formatVersion == 1 && maxAllowableTimeV1.isBefore(
                tbDataEntry.dateTime))) {
            result.addSyncDirButNoOperationalData(currRoot.getName(), currDeploymentPerDevice.device,
                                                  currDeploymentPerDevice.deployment, currVillage,
                                                  currTalkingBook, syncDir.getName());
            logger.debug(String.format(
                    "    directory in TalkingBookData with no matching OperationalData: %s",
                    syncDir.getName()));
            validationErrors.add(new NoMatchingTbDataError(syncDirId.dirName, syncDir,
                                                           manifest.formatVersion == 1 ?
                                                           SyncDirId.SYNC_VERSION_1 :
                                                           SyncDirId.SYNC_VERSION_2));
        } else {
            SyncDirId previousSyncDir = foundSyncDirs.put(tbDataEntry, tbDataEntry);
            if (previousSyncDir != null) {
                validationErrors.add(new MultipleTbDatasMatchError(tbDataEntry.dirName,
                                                                   tbDataInfo.get(
                                                                           tbDataEntry).deviceName));
            }

            OperationalInfo operationalInfo = tbDataInfo.get(tbDataEntry);
            List<IncorrectPropertyValue> incorrectPropertyValues = new LinkedList<>();

            if (!currVillage.equalsIgnoreCase(operationalInfo.inVillage)
                    && !"UNKNOWN".equalsIgnoreCase(operationalInfo.inVillage)) {
                incorrectPropertyValues.add(
                        new IncorrectPropertyValue("Village", operationalInfo.inVillage,
                                                   currVillage));
            }

            if (!currTalkingBook.equalsIgnoreCase(operationalInfo.inTalkingBook) &&
                    !currTalkingBook.equalsIgnoreCase("UNKNOWN")  &&
                    // Next line to to an awful bug that was live for about 3 months in 2016, assigning "-- to be assigned --"
                    // as serial numbers. The outTalkingBook is valid in those cases, however.
                    !currTalkingBook.equalsIgnoreCase("-- to be assigned --") &&
                    !currTalkingBook.equalsIgnoreCase(operationalInfo.outTalkingBook)) {
                incorrectPropertyValues.add(
                        new IncorrectPropertyValue("Talking Book", operationalInfo.outTalkingBook,
                                                   currTalkingBook));
            }

            if (!currDeploymentPerDevice.deployment.equalsIgnoreCase(
                    operationalInfo.inDeploymentId)) {
                incorrectPropertyValues.add(
                        new IncorrectPropertyValue("Deployment Id", operationalInfo.inDeploymentId,
                                                   currDeploymentPerDevice.deployment));
            }

            if (!currDeploymentPerDevice.device.equalsIgnoreCase(operationalInfo.deviceName)) {
                incorrectPropertyValues.add(
                        new IncorrectPropertyValue("Device", operationalInfo.deviceName,
                                                   currDeploymentPerDevice.device));
            }

            if (!incorrectPropertyValues.isEmpty()) {
                result.addIncorrectPropertyValues(currRoot.getName(), currDeploymentPerDevice.device,
                                                  currDeploymentPerDevice.deployment, currVillage,
                                                  currTalkingBook, syncDir.getName());
                File destFile = FileUtils.getFile(currDeploymentPerDevice.getRoot(currRoot, format),
                                                  operationalInfo.inVillage,
                                                  operationalInfo.inTalkingBook, syncDirId.dirName);

                validationErrors.add(
                        new InvalidSyncDirError(syncDir, destFile, incorrectPropertyValues));
            }
        }

        //Validate Manifest File
        SyncRange range = manifest.devices.get(currDeploymentPerDevice.device);
        Date startTime = range != null ? range.getStartTime() : null;
        Date endTime = range != null ? range.getEndTime() : null;
        if (startTime == null || endTime == null) {

            if (!deviceIncorrectlyInManifest.contains(currDeploymentPerDevice.device)) {
                validationErrors.add(
                        new ManfestDoesNotContainDevice(currDeploymentPerDevice.device));
                deviceIncorrectlyInManifest.add(currDeploymentPerDevice.device);
            }
        } else {
            Date syncDirDate = syncDirId.dateTime.toDate();
            int startCompare = startTime.compareTo(syncDirDate);
            int endCompare = endTime.compareTo(syncDirDate);

            if (startCompare > 0 || endCompare < 0) {
                if (!deviceIncorrectlyInManifest.contains(currDeploymentPerDevice.device)) {
                    validationErrors.add(
                            new ManifestHasWrongDeviceRanges(currDeploymentPerDevice.device,
                                                             startTime, endTime, syncDirId.dateTime,
                                                             syncDir));
                    deviceIncorrectlyInManifest.add(currDeploymentPerDevice.device);
                }
            }
        }
    }

    private SyncDirId findMatchingTbDataEntry(SyncDirId syncDirId) {

        SyncDirId currSync = null;
        //If the manifest is from the older version, just need to verify there is an entry shortly after the syncDir time, and
        //that there are not duplicate directories going to the same one.  For the newer format, there needs to be an exact match.
        if (manifest.formatVersion == 1) {
            NavigableMap<SyncDirId, OperationalInfo> laterOperations = tbDataInfo.tailMap(syncDirId,
                                                                                          true);
            Iterator<SyncDirId> iter = laterOperations.navigableKeySet().iterator();

            while (iter.hasNext()) {
                currSync = iter.next();
                if (currDeploymentPerDevice.device.equals(tbDataInfo.get(currSync).deviceName)) {
                    break;
                } else {
                    currSync = null;
                }
            }

        } else {
            if (syncDirId.version != SyncDirId.SYNC_VERSION_2) {
                validationErrors.add(new InvalidSyncDirFormat());
            }

            if (tbDataInfo.containsKey(syncDirId)) {
                //Make sure currSync is the same identity as in the map for the validation test below
                currSync = tbDataInfo.tailMap(syncDirId, true).firstKey();
            }
        }

        return currSync;
    }

    @Override
    public void endProcessing() throws Exception {
        String project = currRoot.getName();
        super.endProcessing();

        //Check to see if anything was in the TBData files, but not on the file systems
        Set<SyncDirId> idsInTbDataNotUsed = Sets.difference(tbDataInfo.keySet(),
                                                            foundSyncDirs.keySet());
        if (!idsInTbDataNotUsed.isEmpty()) {
            List<NonMatchingTbDataEntry> nonMatchingTbDataEntries = new ArrayList<>();
            for (SyncDirId id : idsInTbDataNotUsed) {
                OperationalInfo opInfo = tbDataInfo.get(id);
                result.addOperationalDataButNoSyncDir(project, opInfo.deviceName, opInfo.outDeploymentId,
                                                      opInfo.outVillage, opInfo.outTalkingBook, opInfo.syncDirName);
                logger.debug(String.format(
                        "    operationalData entry with no TalkingBookData directory: %s",
                        opInfo));
                nonMatchingTbDataEntries.add(new NonMatchingTbDataEntry(id, opInfo));
            }

            validationErrors.add(new UnmatchedTbDataEntries(nonMatchingTbDataEntries));
        }

    }

    @Override
    public void creatingManifest(File root) {
        super.creatingManifest(root);
        System.out.print(String.format(PROGRESS_CREATING_MANIFEST, root.getName()));
    }

    @Override
    public void createdManifest() {
        super.createdManifest();
        System.out.print(String.format(PROGRESS_CREATED_MANIFEST));
    }

}
