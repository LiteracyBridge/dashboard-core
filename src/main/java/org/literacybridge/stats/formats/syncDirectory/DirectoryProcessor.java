package org.literacybridge.stats.formats.syncDirectory;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.literacybridge.dashboard.processes.ContentUsageUpdateProcess;
import org.literacybridge.stats.DirectoryIterator;
import org.literacybridge.stats.api.TalkingBookDataProcessor;
import org.literacybridge.stats.formats.exceptions.CorruptFileException;
import org.literacybridge.stats.formats.flashData.FlashData;
import org.literacybridge.stats.formats.flashData.SystemData;
import org.literacybridge.stats.formats.logFile.LogFileParser;
import org.literacybridge.stats.formats.statsFile.StatsFile;
import org.literacybridge.stats.formats.tbData.TbDataParser;
import org.literacybridge.stats.model.DeploymentPerDevice;
import org.literacybridge.stats.model.DirectoryFormat;
import org.literacybridge.stats.model.ProcessingContext;
import org.literacybridge.stats.model.StatsPackageManifest;
import org.literacybridge.stats.model.SyncDirId;
import org.literacybridge.stats.model.SyncProcessingContext;
import org.literacybridge.dashboard.dbTables.TbDataLine;
import org.literacybridge.stats.processors.AbstractDirectoryProcessor;
import org.literacybridge.utils.FsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class DirectoryProcessor extends AbstractDirectoryProcessor {
  public static final Map<String, String> CATEGORY_MAP = ImmutableMap.<String, String>builder()
    .put("1", "AGRIC")
    .put("1-2", "LIVESTOCK")
    .put("2", "HEALTH")
    .put("9", "FEEDBACK")
    .put("0", "OTHER")
    .put("$0-1", "TB")
    .build();
  private static final Pattern ARCHIVED_LOG_PATTERN = Pattern.compile("log_(.*).txt");
  protected static final Logger logger = LoggerFactory.getLogger(DirectoryProcessor.class);
  private static final String statExtension = ".stat";

  //Stats files don't have any "."s in them (because they have no file extensions) (other than ".stat", you mean?)
  private static final Pattern STATS_FILE_PATTERN = Pattern.compile("(.*)" + statExtension);

  private static final String PROGRESS_ROOT_FORMAT    = "%s%n";
  private static final String PROGRESS_TBDATA_FORMAT  = "  parsing %s%n";
  private static final String PROGRESS_DEPL_FORMAT    = "    %s (%s)%n";
  private static final String PROGRESS_VILLAGE_FORMAT = "      %s%n";
  private static final String PROGRESS_TB_FORMAT      = "        %s%n";

  private final Collection<TalkingBookDataProcessor> dataProcessorEventListeners;
  private ProcessingContext currProcessingContext;
  private Set<String> processedLogFiles = new HashSet<>();

    public DirectoryProcessor(
      Collection<TalkingBookDataProcessor> dataProcessorEventListeners,
      ContentUsageUpdateProcess.UpdateUsageContext context) {
      super(context);
    this.dataProcessorEventListeners = dataProcessorEventListeners;
  }

  static public int runCallbacksOnLogFile(File file, LogFileParser parser) throws IOException {
    FileInputStream fis = new FileInputStream(file);
    try {
      return parser.parse(file.getAbsolutePath(), fis);
    } finally {
      IOUtils.closeQuietly(fis);
    }

  }

  /**
   * Loads a FlashData file from a given sync directory.  This file was introduced
   * in a more recent update, so will not be around for all updates.
   *
   * @param syncDir
   * @return {@null} if the file is not there
   * @throws java.io.IOException
   */
  static public FlashData loadFlashDataFile(File syncDir) throws IOException {
    final File flashDataFile = new File(syncDir, FsUtils.FsAgnostify("statistics/flashData.bin"));

    FlashData retVal = null;
    FileInputStream fis = null;
    try {
      if (flashDataFile.exists() && flashDataFile.length() == 6708) {
        fis = new FileInputStream(flashDataFile);
        retVal = FlashData.parseFromStream(fis);

        LinkedList<String> errors = new LinkedList<>();
        if (!retVal.isValid(errors)) {
          logger.error("Flashdata file look possibly corrupt.  Errors=" + StringUtils.join(errors, "; ") + "Path=" + flashDataFile.getCanonicalPath() + " FlashData=" + retVal.toString());
        }
      }
    } finally {
      IOUtils.closeQuietly(fis);
    }

    return retVal;
  }

    /**
     * Loads a "deployment.properties" if one exists. This file is a more modern, complete, and
     * accurate way of determining information about the deployment to a Talking Book. The 
     * properties to be found in the file are:
     *   PROJECT            -- the project being read from the talking book
     *   DEPLOYMENT         -- the deployment that is on the talking book
     *   PACKAGE            -- which package from the deployment, on this TB
     *   COMMUNITY          -- community name, in form of directory name
     *   RECIPIENTID        -- the unique recipient id for the community/group
     *   TALKINGBOOKID      -- the TB serial number
     *   TIMESTAMP          -- timestamp when the content was deployed to the TB
     *   TESTDEPLOYMENT     -- "true"/"false": was the deployment for purposes of testing?
     *   USERNAME           -- the name of the user performing the update
     *   LOCATION           -- "Other", "Community", "Wa office", etc.
     *   COORDINATES        -- if known, the lat/long where the deployment physically performed
     *   TBCDID             -- the TB-Loader id that performed the deployment
     *   NEWTBID            -- "true"/"false": was this TB's SRN newly allocated for this deployment?
     *   FIRMWARE           -- firmware version
     *
     * @param syncDir The so-called "sync dir". The name is a timestamp, and the directory contains
     *                a copy of the contents of the Talking Book file system.
     * @return a Properties object, or null if there is no such file.
     */
  static public Properties loadDeploymentProperties(File syncDir) {
      File propertiesFile = new File(syncDir, FsUtils.FsAgnostify("system/deployment.properties"));
      if (propertiesFile.exists()) {
          Properties result = new Properties();
          try (FileInputStream propertiesStream = new FileInputStream(propertiesFile) ) {
              result.load(propertiesStream);
              return result;
          } catch (Exception e) {
              // Ignore exception, and continue without properties file.
          }
      }
      return null;
  }

    /**
     * Looks for a ".pkg" file on the TB file system.
     * @param syncDir Root of the TB file system (copy thereof).
     * @param defaultContentId default if none is found.
     * @return the package name.
     */
  static public String findContentIdByPackage(File syncDir, String defaultContentId) {
      return findValueByMarkerFile(syncDir, ".pkg", defaultContentId);
  }
  static public String findContentIdByPackage(File syncDir) {
      return findContentIdByPackage(syncDir, null);
  }

    /**
     * Looks for a ".prj" file on the TB file system.
     * @param syncDir Root of the TB file system (copy thereof).
     * @return the project name, or null if not found.
     */
    static public String findProjectByTbFs(File syncDir) {
        return findValueByMarkerFile(syncDir, ".prj", null);
    }

    /**
     * Looks for a file with a particular extension in the system directory of the TB file system.
     * These are files like 'foo.prj' or bar.pkg', where the name is the value.
     * @param syncDir Root of the TB file system (copy thereof).
     * @param extension of the marker file.
     * @param defaultValue default if none is found.
     * @return the first file with the extension, or defaultValue is none is found.
     */
    static public String findValueByMarkerFile(File syncDir, String extension, String defaultValue) {
        File systemDir = new File(syncDir, "system");
        if (!extension.startsWith(".")) throw new IllegalArgumentException("Extension must begin with '.'");
        String result = defaultValue;
        if (systemDir.exists()) {
            FileFilter fileFilter = new WildcardFileFilter("*"+extension);
            File[] files = systemDir.listFiles(fileFilter);
            if (files != null && files.length > 0) {
                String name = files[0].getName();
                result = name.substring(0, name.lastIndexOf('.'));
            }
        }
        return result;
    }

    static public LocalDateTime findLastUpdateTime(File syncDir) {
        File systemDir = new File(syncDir, "system");
        File lastUpdateFile = new File(systemDir, "last_updated.txt");
        if (lastUpdateFile.exists()) {
            try (FileInputStream lastUpdateStream = new FileInputStream(lastUpdateFile);
                InputStreamReader lastUpdateReader = new InputStreamReader(lastUpdateStream);
                BufferedReader bufferedReader = new BufferedReader(lastUpdateReader)) {
                String lastUpdate = bufferedReader.readLine().trim();
                return DirectoryIterator.lbToIsoTimestamp(lastUpdate);
            } catch (Exception e) {
                // Ignore exception, and continue without value.
            }
        }
        return null;
    }

    static public String findValueInSysData(File syncDir, String key) {
        File systemDir = new File(syncDir, "system");
        File lastUpdateFile = new File(systemDir, "sysdata.txt");
        Pattern pattern = Pattern.compile("^" + key + ":([a-zA-Z0-9_-]+)");
        if (lastUpdateFile.exists()) {
            try (FileInputStream lastUpdateStream = new FileInputStream(lastUpdateFile);
                InputStreamReader lastUpdateReader = new InputStreamReader(lastUpdateStream);
                BufferedReader bufferedReader = new BufferedReader(lastUpdateReader)) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    Matcher matcher = pattern.matcher(line.trim());
                    if (matcher.matches()) {
                        return matcher.group(1);
                    }
                }
            } catch (Exception e) {
                // Ignore exception, and continue without properties file.
            }
        }
        return null;

    }

  /**
   * Creates a processing context.  There are two ways to do this:
   * <p/>
   * <ol>
   * <li>Use the flashData.bin file and get the info there there.  This is the MOST reliable way to get some stat, since this info comes from the NOR flash on the
   * device, which tends to have corruption a lot less.  The only problem is that this is a newer mechanism and so there are a lot of times this file does not exist.
   * </li>
   * <li>
   * Use the filepath conventions to figure this out.  This way always exists, but this information ultimately comes from memory on the device that we have seen
   * corruption occur in.  However, for some information such as talkingBook ID, contentUpdate and village name, this can be the best way, because we have other
   * mechanisms to fix corruption here.
   * </li>
   * </ol>
   *
   * @param syncDevice    The name of the device this information is being synched from.  THis is NOT the TalkingBook, this is the laptop or
   *                      tablet used to sync many talking books. The TB-Loader ID, aka "tbcdid".
   * @param syncDir       The directory this sync is occuring from. A copy of the TB disk image is in this directory.
   * @param talkingBookId the ID of the talking book, as determined from the file system
   * @param deployment    The Deployment name, from the File System directory structure.
   * @param villageName   the village name the talking book was deployed in, as determined from the file system
   * @param flashData     the flashdata file, if it exists for this sync.
   * @param deploymentProperties the deployment.properties file, if it is present on the TB file system.
   * @return
   */
   public SyncProcessingContext determineProcessingContext(
      String syncDevice,
      File syncDir,
      String talkingBookId,
      String deployment,
      String villageName,
      @Nullable FlashData flashData,
      @Nullable Properties deploymentProperties) {

      String project = null;
      String packageName = null;
      String recipientId = null;
      LocalDateTime deploymentTime = null;

      // Get the package name. Use the deployment name if there isn't a discernible package
      if (deploymentProperties != null) {
          project = deploymentProperties.getProperty("PROJECT", null);
          deployment = deploymentProperties.getProperty("DEPLOYMENT", deployment);
          packageName = deploymentProperties.getProperty("PACKAGE", null);
          villageName = deploymentProperties.getProperty("COMMUNITY", villageName);
          talkingBookId = deploymentProperties.getProperty("TALKINGBOOKID", talkingBookId);
          recipientId = deploymentProperties.getProperty("RECIPIENTID", null);
          String deploymentTimeStr = deploymentProperties.getProperty("TIMESTAMP", null);
          if (deploymentTimeStr != null) {
              SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSSX");
              Date date;
              try {
                  date = simpleDateFormat.parse(deploymentTimeStr);
                  deploymentTime = new LocalDateTime(date, DateTimeZone.UTC);
              } catch (ParseException e) {
                  // ignore: we can't parse the time.
              }
          }
      }

      // If we didn't get project from deployment properties, try to get from marker file, else file system.
       if (project == null) {
          project = findProjectByTbFs(syncDir);
       }
       if (project == null) {
          project = findValueInSysData(syncDir, "PROJECT");
       }
       if (project == null) project = currRoot.getName();

      // If we didn't get the Package from the deployment properties, try to get it from the TB
      if (packageName == null) {
          // From the flash data on the TB file system.
          if (flashData != null) {
              final SystemData systemData = flashData.getSystemData();
              String pkg = systemData.getContentPackage();
              if (StringUtils.isNotEmpty(pkg) && pkg.matches("^\\p{ASCII}*$")) {
                  packageName = pkg;
              }
          }
          // Or else from the .pkg file. If not .pkg file, fall back to the deployment.
          if (packageName == null) {
              packageName = findContentIdByPackage(syncDir);
          }
          if (packageName == null) {
              packageName = findValueInSysData(syncDir, "IMAGE");
          }
          if (packageName == null) packageName = deployment; // last gasp effort.
      }

      // If we didn't get the deployment time from deployment properties, try to get it from the TB
       if (deploymentTime == null) {
           deploymentTime = findLastUpdateTime(syncDir);
       }


    SyncProcessingContext retVal = new SyncProcessingContext(syncDir.getName(),
      talkingBookId,
      villageName,
      packageName,
      deployment,
      project,
      syncDevice,
      recipientId,
      deploymentTime);

    return retVal;
  }

    @Override
    public boolean startProcessing(@Nonnull File root,
        StatsPackageManifest manifest,
        @Nonnull DirectoryFormat format) throws Exception
    {
        System.out.print(String.format(PROGRESS_ROOT_FORMAT, root.getName()));
        return super.startProcessing(root, manifest, format);
    }

    @Override
    public boolean startDeviceAndDeployment(DeploymentPerDevice deploymentPerDevice)
        throws Exception
    {
        System.out.print(String.format(PROGRESS_DEPL_FORMAT,
            deploymentPerDevice.deployment,
            deploymentPerDevice.device));
        return super.startDeviceAndDeployment(deploymentPerDevice);
    }

    @Override
    public boolean startVillage(String village) throws Exception {
        System.out.print(String.format(PROGRESS_VILLAGE_FORMAT, village));
        return super.startVillage(village);
    }

    @Override
  public boolean startDeviceOperationalData(@Nonnull String device) {
    return true;
  }

  @Override
  public void processTbDataFile(File tbdataFile, boolean includesHeaders) throws IOException {
    TbDataParser parser = new TbDataParser();
      System.out.print(String.format(PROGRESS_TBDATA_FORMAT, tbdataFile.getName()));
    List<TbDataLine> lines = parser.parseTbDataFile(tbdataFile, true);
    for (TbDataLine line : lines) {
      for (TalkingBookDataProcessor processor : dataProcessorEventListeners) {
        processor.processTbDataLine(line);
      }
    }
  }

  @Override
  public boolean startTalkingBook(String talkingBook) throws Exception {
    super.startTalkingBook(talkingBook);
      System.out.print(String.format(PROGRESS_TB_FORMAT, talkingBook));

    currTalkingBook = talkingBook;

    currProcessingContext = new ProcessingContext(currTalkingBook, currVillage, currDeploymentPerDevice.deployment, currDeploymentPerDevice.device, null);
    for (TalkingBookDataProcessor processor : dataProcessorEventListeners) {
      processor.onTalkingBookStart(currProcessingContext);
    }

    processedLogFiles.clear();
    return true;
  }

  @Override
  public void endTalkingBook() {
    for (TalkingBookDataProcessor processor : dataProcessorEventListeners) {
      processor.onTalkingBookEnd(currProcessingContext);
    }

    currProcessingContext = null;
    processedLogFiles.clear();
    super.endTalkingBook();
  }

  @Override
  public void processSyncDir(SyncDirId syncDirId, File syncDir) throws Exception {
      final FlashData flashData = loadFlashDataFile(syncDir);
      final Properties deploymentProperties = loadDeploymentProperties(syncDir);
      final SyncProcessingContext syncProcessingContext = determineProcessingContext(
          currDeploymentPerDevice.device,
          syncDir,
          currTalkingBook,
          currDeploymentPerDevice.deployment,
          currVillage,
          flashData,
          deploymentProperties);

      for (TalkingBookDataProcessor processor : dataProcessorEventListeners) {
          processor.onSyncProcessingStart(syncProcessingContext);
      }
            
      if (flashData != null) {
          processFlashData(syncProcessingContext, flashData);
      }

      processSyncDir(syncDir, syncProcessingContext, processedLogFiles, true);

      for (TalkingBookDataProcessor processor : dataProcessorEventListeners) {
          processor.onSyncProcessingEnd(syncProcessingContext);
      }

  }

    /**
   * Processes a FlashData file to call all the registered callbacks.
   *
   * @param syncProcessingContext context this processing occurs in
   * @param flashData             the flashdata file.  If this is null, this function will be a no-op.
   */
  public void processFlashData(final SyncProcessingContext syncProcessingContext, @Nullable final FlashData flashData) throws
    IOException {
    if (flashData != null) {
      for (TalkingBookDataProcessor processor : dataProcessorEventListeners) {
        processor.processFlashData(syncProcessingContext, flashData);
      }
    }
  }

  /**
   * Processes a Sync directory.  This will process the following:
   * <p/>
   * <ul>
   * <li>if processInProcessLog is true:  log/log.txt</li>
   * <li>log-archive/log_*.txt</li>
   * <li>statistics/stats/*</li>
   * </ul>
   * <p/>
   * In addition, it will not processes files in processedFiles, and will add the files it processes into it when done.  Both the
   * {@code processedFiles} + {@code processInProcessLog} parameters are meant to prevent multiple counting of data when merging unsuccessful syncs.
   *
   * @param syncDir
   * @param syncProcessingContext
   * @param processedFiles
   * @param processInProcessLog
   */
  public void processSyncDir(final File syncDir, final SyncProcessingContext syncProcessingContext,
                             final Set<String> processedFiles, final boolean processInProcessLog) throws
    IOException {
    int numLogFiles = 0;
    int numLogFilesWithErrors = 0;
    int numLogFileErrors = 0;

    //Create a list of LogFileParsers that take the callback interfaces and the syncProcessingContexts.
    LogFileParser parser = new LogFileParser(dataProcessorEventListeners, syncProcessingContext);

    //Process the current log and the flashData files, if this is the latest dir
    if (processInProcessLog) {
      final File logFile = new File(new File(syncDir, "log"), "log.txt");
      if (logFile.canRead()) {
          numLogFiles++;
          numLogFileErrors += processLogFile(logFile, parser, processedFiles);
          if (numLogFileErrors > 0) numLogFilesWithErrors++;
      }
    }

    //Process all the Archive Files
    final File logArchives = new File(syncDir, "log-archive");
    if (logArchives.isDirectory()) {
      final Iterator<File> archivedLogFiles = FileUtils.iterateFiles(logArchives,
        new RegexFileFilter(ARCHIVED_LOG_PATTERN),
        FalseFileFilter.FALSE);
      while (archivedLogFiles.hasNext()) {
        final File archivedFile = archivedLogFiles.next();
        int numErrors = processLogFile(archivedFile, parser, processedFiles);
        numLogFiles++;
        if (numErrors > 0)
            numLogFilesWithErrors++;
        numLogFileErrors += numErrors;
      }
    }
      result.addCountLogFiles(currRoot.getName(), currDeploymentPerDevice.device, currDeploymentPerDevice.deployment, currVillage, currTalkingBook, numLogFiles);
      result.addCountLogFilesWithErrors(currRoot.getName(), currDeploymentPerDevice.device, currDeploymentPerDevice.deployment, currVillage, currTalkingBook, numLogFilesWithErrors);
      result.addCountLogFileErrors(currRoot.getName(), currDeploymentPerDevice.device, currDeploymentPerDevice.deployment, currVillage, currTalkingBook, numLogFileErrors);

      //Process all the Stats files
      final File statDir = new File(syncDir, "statistics");
      // TODO: replace the line above with the line below after processing 2014-3
      // (The stats folder is no longer used on TB; all stats in statistics.
      //  TB Loader has been compensating by copying into a stats subdirectory
      //  just for backward compatibilty; but that compensation appears not to
      //  have happened in first MEDA update).
      //final File statDir = new File(syncDir, "statistics");
      if (statDir.canRead()) {
          if (statDir.isDirectory()) {
              final Iterator<File> statsFiles = FileUtils.iterateFiles(statDir,
                  new RegexFileFilter(STATS_FILE_PATTERN),
                  FalseFileFilter.FALSE);
              while (statsFiles.hasNext()) {
                  doProcessStatsFile(syncProcessingContext, statsFiles.next());
              }
          } else {
              result.addCorruptStatisticsDir(currRoot.getName(), currDeploymentPerDevice.device,
                  currDeploymentPerDevice.deployment, currVillage,
                  currTalkingBook, syncDir.getName());
              logger.error(statDir.getAbsolutePath() + " is NOT a directory.");
          }
      } else {
          result.addCorruptStatisticsDir(currRoot.getName(), currDeploymentPerDevice.device,
              currDeploymentPerDevice.deployment, currVillage,
              currTalkingBook, syncDir.getName());
          logger.error("Cannot read " + statDir.getAbsolutePath());
      }
  }

  public int processLogFile(File file, LogFileParser parser, Set<String> processedFiles) {
    int numErrors = 0;
    final String fileProcessingName = file.getParent() + "/" + file.getName();
    // TODO: Does this do anything?
    if (!processedFiles.contains(fileProcessingName)) {
      try {
        numErrors = runCallbacksOnLogFile(file, parser);
        processedFiles.add(fileProcessingName);
      } catch (IOException ioe) {
        final String errorString = String.format("Unable to process %s.  Error=%s", file.getAbsolutePath(),
          ioe.getMessage());
        logger.error(errorString, ioe);
        numErrors = 1; // the whole file...
      }
    }
    return numErrors;
  }

  public void doProcessStatsFile(final SyncProcessingContext syncProcessingContext, final File file) {

    try {
      FileInputStream fis = new FileInputStream(file);

      try {
        StatsFile statsFile = StatsFile.read(fis);
        for (TalkingBookDataProcessor processor : dataProcessorEventListeners) {
          int delimeterPosition = file.getName().indexOf('^');
          String contentId = file.getName().substring(delimeterPosition + 1, file.getName().length() - statExtension.length());
          String packageName = file.getName().substring(0, delimeterPosition);  // not used right now but should be checked against processing context
          processor.processStatsFile(syncProcessingContext, contentId, statsFile);
        }
      } catch (CorruptFileException e) {
        for (TalkingBookDataProcessor processor : dataProcessorEventListeners) {
          processor.markStatsFileAsCorrupted(syncProcessingContext, file.getName(), e.getMessage());
        }
      }
    } catch (IOException e) {
      logger.error("Could not load stats file", e);
    }

  }

}
