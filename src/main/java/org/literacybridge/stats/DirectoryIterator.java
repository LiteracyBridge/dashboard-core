package org.literacybridge.stats;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.codehaus.jackson.map.ObjectMapper;
import org.literacybridge.dashboard.ProcessingResult;
import org.literacybridge.stats.api.DirectoryCallbacks;
import org.literacybridge.stats.model.DeploymentId;
import org.literacybridge.stats.model.DeploymentPerDevice;
import org.literacybridge.stats.model.DirectoryFormat;
import org.literacybridge.stats.model.StatsPackageManifest;
import org.literacybridge.stats.model.SyncDirId;
import org.literacybridge.stats.processors.ManifestCreationCallbacks;
import org.literacybridge.utils.FsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

/**
 * This class is responsible for navigating the directory structure in a Stats Update Package.
 */
public class DirectoryIterator {
    // groups of letters and numbers separated by dashes
    private static final Pattern UPDATE_PATTERN = Pattern.compile("(\\w+-?)+");
    private static final Pattern TBDATA_PATTERN = Pattern.compile("tbData-(\\d+)-(\\d+)-(\\d+).*",
                                                                  Pattern.CASE_INSENSITIVE);
    public static final Pattern SYNC_TIME_PATTERN_V1 = Pattern.compile(
            "(\\d+)m(\\d+)d(\\d+)h(\\d+)m(\\d+)s", Pattern.CASE_INSENSITIVE);
    public static final Pattern SYNC_TIME_PATTERN_V2 = Pattern.compile(
            "(\\d+)y(\\d+)m(\\d+)d(\\d+)h(\\d+)m(\\d+)s-(.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern TBDATA_PATTERN_V2 = Pattern.compile(
            "tbData-v(\\d+)-(\\d+)y(\\d+)m(\\d+)d-(.*).csv", Pattern.CASE_INSENSITIVE);
    private static final String MANIFEST_FILE_NAME = "StatsPackageManifest.json";

    //tbData-v00-2014y05m02d-9d8839de.csv

    private static final String TBLOADER_LOG_DIR = "logs";
    private static final String TBDATA_DIR_V2 = "tbdata";
    public static final String UPDATE_ROOT_V1 = "collected-data";
    private static final String DEVICE_OPERATIONS_DIR_ARCHIVE_V2 = "OperationalData";
    public static final String TALKING_BOOK_ROOT_V2 = "TalkingBookData";
    private static final ObjectMapper mapper = new ObjectMapper();
    protected static final Logger logger = LoggerFactory.getLogger(DirectoryIterator.class);
    private final boolean strict;
    private final File[] rootFiles;
    private DirectoryFormat format;
    private ProcessingResult result;

    public DirectoryIterator(File root, DirectoryFormat format, boolean strict, ProcessingResult result) {
        this.rootFiles = rootInFunnyZip(root);
        this.strict = strict;
        this.format = format;
        this.result = result;
    }

    /**
     * Our Root detection has just gotten a lot more complicated.  There are a few
     * things that can happen:
     * <p/>
     * The root can be the root of the zip file
     * The root can be under the collected-data/* directory
     * There can be multiple roots under either the zip file's root or the collected-data
     * <p/>
     * THere are some zip files that are not properly formed, and start with
     * collected-data/*.  This will check to see if this is one of those zips.
     *
     * @param zipRoot the root directory the zip was expanded to
     * @return the proper root directory to use for processing
     */
    public static File[] rootInFunnyZip(File zipRoot) {

        File root = zipRoot;
        File collectedDataFile = new File(root, UPDATE_ROOT_V1);
        if (collectedDataFile.exists())
            root = collectedDataFile;
        else {
            File[] dbAccounts = root.listFiles();
            File dropboxAccount = null;
            for (File f : dbAccounts) {
                if (f.isDirectory() && !f.isHidden() && !f.getName().startsWith("_") && !f.getName()
                        .startsWith(".")) { // avoid folders like __MACOSX
                    dropboxAccount = f;
                    break;
                }
            }
            if (dropboxAccount != null && dropboxAccount.exists()) {
                File altCollectedDataFile = new File(dropboxAccount, UPDATE_ROOT_V1);
                if (altCollectedDataFile.exists()) {
                    File[] projects = altCollectedDataFile.listFiles();
                    File project = null;
                    for (File f : projects) {
                        if (f.isDirectory() && !f.isHidden() && !f.getName().startsWith("_") && !f.getName().startsWith(".")) { // avoid folders like __MACOSX
                            // only one directory should match; if more than one, then this is the old style without a project dir
                            if (project == null) {
                                project = f;
                            } else {
                                project = null;
                                break;
                            }
                        }
                    }
                    if (project != null && project.exists()) {
                        System.out.println("   project directory listed");
                        root = project;
                    } else {
                        System.out.println("   OLD-no project directory listed");
                        root = altCollectedDataFile;
                    }
                }
            }
        }

        File[] processingRoots = new File[] { root };

        //If there is no talkingbookdata, then there are multiple roots
        File talkingbookdata = FsUtils.FileIgnoreCase(root, TALKING_BOOK_ROOT_V2);
        if (!talkingbookdata.exists()) {
            processingRoots = root.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    if (pathname.isDirectory() && !pathname.isHidden() && !pathname.getName().startsWith("_") && !pathname.getName().startsWith(
                            ".")) // avoid folders like __MACOSX
                        return true;
                    else
                        return false;
                }
            });
        }
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.println(String.format("Size: %s, Start Time: %s", getBytesString(FileUtils.sizeOfDirectory(root)),
                                         sdf.format(Calendar.getInstance().getTime())));
        return processingRoots;
    }

    private static String getBytesString(long bytes) {
        String[] quantifiers = new String[] { "KiB", "MiB", "GiB", "TiB" };
        double sizeNum = bytes;
        for (int i = 0; ; i++) {
            if (i >= quantifiers.length) {
                return "Too Much";
            }
            sizeNum /= 1024;
            if (sizeNum <= 999) {
                return String.format("%.2f %s", sizeNum, quantifiers[i]);
            }
        }
    }

    private static File getManifestFile(File root) {
        return new File(root, MANIFEST_FILE_NAME);
    }

    private static File getTbDataDir(File root, String device, DirectoryFormat format) {
        File retVal;

        if (format == DirectoryFormat.Sync) {
            retVal = FsUtils.FileIgnoreCase(root, device, UPDATE_ROOT_V1);
        } else {
            retVal = FsUtils.FileIgnoreCase(root, DEVICE_OPERATIONS_DIR_ARCHIVE_V2, device,
                                            TBDATA_DIR_V2);
        }

        return retVal;
    }

    public static File getTbLoaderLogFileDir(File root, String device, DirectoryFormat format) {
        File retVal;

        if (format == DirectoryFormat.Sync) {
            retVal = FsUtils.FileIgnoreCase(root, device, UPDATE_ROOT_V1, TBLOADER_LOG_DIR);
        } else {
            retVal = FsUtils.FileIgnoreCase(root, DEVICE_OPERATIONS_DIR_ARCHIVE_V2, device,
                                            TBLOADER_LOG_DIR);
        }

        return retVal;
    }

    private static StatsPackageManifest readInManifest(File manifestFile, DirectoryFormat format,
                                                       boolean strict) throws IOException {

        StatsPackageManifest manifest = mapper.readValue(manifestFile, StatsPackageManifest.class);
        DirectoryFormat manifestFormat = DirectoryFormat.fromVersion(manifest.formatVersion);
        if (format != null && format != manifestFormat) {
            String errorMessage = "Format is set as " + manifestFormat
                    + " in the manifest, but the Directory iterator is created with format=" + format
                    + ".  If the directory will always have a manifest, you can simply create the DirectoryIterator with a null format.";
            if (strict) {
                throw new IllegalArgumentException(errorMessage);
            }
            logger.error(errorMessage);
        }
        format = manifestFormat;

        return manifest;
    }

    public void process(DirectoryCallbacks callbacks) throws Exception {
        for (File currRoot : rootFiles) {
            logger.debug(String.format("project: %s", currRoot.getName()));
            process(currRoot, callbacks);
        }
    }

    protected void process(final File root, DirectoryCallbacks callbacks) throws Exception {
        StatsPackageManifest manifest = null;
        File manifestFile = getManifestFile(root);
        if (manifestFile.exists()) {
            manifest = readInManifest(manifestFile, format, strict);
            format = DirectoryFormat.fromVersion(manifest.formatVersion);
        } else {
            if (format == null) {
                if (strict) {
                    throw new IllegalArgumentException(
                            "No Manifest is set, and no directory format is set.");
                }

                format = DirectoryFormat.Sync;
            }

            System.out.println("Generating manifest.");
            manifest = generateManifest(root, format);
            System.out.println("Done generating manifest, processing resuming.");

        }

        process(root, manifest, callbacks);
    }

    private StatsPackageManifest generateManifest(File root, DirectoryFormat format)
            throws Exception {
        this.format = format;

        logger.debug("Generating manifest");
        ManifestCreationCallbacks manifestCreationCallbacks = new ManifestCreationCallbacks(result);
        process(root, null, manifestCreationCallbacks);
        StatsPackageManifest result = manifestCreationCallbacks.generateManifest(format);
        logger.debug("---------"); //
        logger.debug(String.format("Continue with generated manifest: %s", root.getName()));
        return result;
    }

    public void process(@Nonnull File root, @Nullable StatsPackageManifest manifest, @Nonnull DirectoryCallbacks callbacks) throws Exception {

        if (!root.exists()) {
            throw new IllegalArgumentException("Root directory does not exist: " + root.getCanonicalPath());
        }

        if (callbacks.startProcessing(root, manifest, format)) {

            TreeSet<DeploymentPerDevice> deploymentPerDevices = loadDeviceDeployments(root);

            if (!deploymentPerDevices.isEmpty()) {
                String currDevice = null;
                boolean deviceAlreadyProcessed = false;
                boolean processDevice = false;

                for (DeploymentPerDevice deploymentPerDevice : deploymentPerDevices) {
                    logger.debug(String.format("  device: %s, deployment: %s", deploymentPerDevice.device,
                                               deploymentPerDevice.deployment));
                    if (!deploymentPerDevice.device.equalsIgnoreCase(currDevice)) {

                        if (processDevice) {
                            callbacks.endDeviceOperationalData();
                        }

                        currDevice = deploymentPerDevice.device;
                        deviceAlreadyProcessed = false;
                        processDevice = callbacks.startDeviceOperationalData(currDevice);
                    }

                    if (processDevice) {
                        File tbdataDir = getTbDataDir(root, currDevice, format);
                        //
                        if (!deviceAlreadyProcessed && tbdataDir.exists()) {

                            if (format == DirectoryFormat.Sync) {
                                for (File potential : tbdataDir.listFiles((FilenameFilter) new RegexFileFilter(TBDATA_PATTERN))) {
                                    callbacks.processTbDataFile(potential, false);
                                }
                                for (File potential : tbdataDir.listFiles((FilenameFilter) new RegexFileFilter(TBDATA_PATTERN_V2))) {
                                    callbacks.processTbDataFile(potential, false);
                                }
                            } else {
                                for (File potential : tbdataDir.listFiles((FilenameFilter) new RegexFileFilter(TBDATA_PATTERN_V2))) {
                                    logger.debug(String.format("    operational data: %s", potential.getName()));
                                    callbacks.processTbDataFile(potential, true);
                                }
                            }
                            deviceAlreadyProcessed = true;
                        }
                    }
                }

                if (processDevice) {
                    callbacks.endDeviceOperationalData();
                }
            } else {
                throw new IllegalArgumentException(
                        "No records found in this directory.  Make sure the format in the manifest is correct?  Make sure the directory structure is correct?");
            }

            for (DeploymentPerDevice deploymentPerDevice : deploymentPerDevices) {
                DeploymentId deploymentId = DeploymentId.parseContentUpdate(deploymentPerDevice.deployment);
                if (deploymentId.year == 0 && strict) {
                    throw new IllegalArgumentException("Illegal deployment: " + deploymentId);
                }

                if (callbacks.startDeviceDeployment(deploymentPerDevice)) {
                    processDeviceDeployment(root, deploymentPerDevice.device, deploymentId,
                                            deploymentPerDevice.getRoot(root, format), callbacks);
                    callbacks.endDeviceDeployment();
                }
            }
            callbacks.endProcessing();
        }
    }

    private void processDeviceDeployment(File root, String device, DeploymentId deploymentId, File deviceDeploymentDir, DirectoryCallbacks callbacks)
            throws Exception {
        logger.debug(String.format("    device: %s, deployment: %s", deviceDeploymentDir.getName(),
                                   deploymentId));
        System.out.println(" " + deploymentId.toString());
        for (File villageDir : deviceDeploymentDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
            if (callbacks.startVillage(villageDir.getName().trim())) {
                System.out.println("   " + villageDir.getName());
                processVillage(root, device, deploymentId, villageDir, callbacks);
                callbacks.endVillage();
            }
        }
    }

    private void processVillage(File root, String device, DeploymentId deploymentId, File villageDir,
                                DirectoryCallbacks callbacks) throws Exception {
        logger.debug(String.format("      village: %s", villageDir.getName()));
        for (File talkingBookDir : villageDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
            if (callbacks.startTalkingBook(talkingBookDir.getName().trim())) {
                System.out.println("     " + talkingBookDir.getName().trim());
                processTalkingBook(root, device, deploymentId, villageDir.getName(), talkingBookDir,
                                   callbacks);
                callbacks.endTalkingBook();
            }
        }
    }

    private void processTalkingBook(File root, String device, DeploymentId deploymentId, String village, File talkingBookDir,
                                    DirectoryCallbacks callbacks) throws Exception {
        logger.debug(String.format("        tb: %s", talkingBookDir.getName()));
        FileFilter fileFilter = new WildcardFileFilter("*.zip");
        File[] files = talkingBookDir.listFiles(fileFilter);
        for (File syncZip : files) {
            String filename = syncZip.getName().substring(0, syncZip.getName().length() - 4);
            File folder = new File(talkingBookDir, filename);
            if (folder.exists() && folder.isDirectory()) {
                FileUtils.deleteDirectory(folder);
            }
            try {
                FsUtils.unzip(syncZip, talkingBookDir);
            } catch (ZipException e) {
                result.addCorruptedTalkingBookZip(root.getName(), device, deploymentId.id, village,
                                                  talkingBookDir.getName(), syncZip.getName());
                logger.error("Couldn't unzip synchdir " + syncZip.getName() + "(" + e.getMessage() + ")");
            }
            syncZip.delete();
        }

        for (File syncDir : talkingBookDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
            SyncDirId syncDirId = SyncDirId.parseSyncDir(deploymentId, syncDir.getName().trim());
            if (syncDirId.dateTime != null) {
                if (format == DirectoryFormat.Archive && syncDirId.version == 1 && strict) {
                    throw new IllegalArgumentException(
                            "Directory structure is the newer 'Archive' structure, but the sync directory is using the old format : "
                                    + syncDir.getName());
                }

                callbacks.processSyncDir(syncDirId, syncDir);
            }
        }
    }

    private TreeSet<DeploymentPerDevice> loadDeviceDeployments(final File root) {

        TreeSet<DeploymentPerDevice> retVal = new TreeSet<DeploymentPerDevice>(DeploymentPerDevice.ORDER_BY_DEVICE);

        if (format == DirectoryFormat.Sync) {
            for (File candidateDevice : root.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
                File collectedData = new File(candidateDevice, UPDATE_ROOT_V1);
                if (collectedData.exists() && collectedData.isDirectory()) {
                    for (File deploymentDir : collectedData.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
                        if (UPDATE_PATTERN.matcher(deploymentDir.getName()).matches()) {
                            retVal.add(new DeploymentPerDevice(deploymentDir.getName(),
                                                               candidateDevice.getName()));
                        }
                    }
                }
            }
        } else {
            File talkingBookData = FsUtils.FileIgnoreCase(root, TALKING_BOOK_ROOT_V2);
            if (talkingBookData.exists()) {  // in some cases, there may just be an OperationalData dir but no TalkingBookData
                for (File deploymentDir : talkingBookData.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
                    if (UPDATE_PATTERN.matcher(deploymentDir.getName()).matches() || deploymentDir.getName().equalsIgnoreCase("UNKNOWN")) {
                        for (File device : deploymentDir.listFiles((FileFilter) DirectoryFileFilter.DIRECTORY)) {
                            retVal.add(new DeploymentPerDevice(deploymentDir.getName(), device.getName()));
                        }
                    }
                }
            }
        }

        return retVal;
    }

    public DirectoryFormat getFormat() {
        return format;
    }
}
