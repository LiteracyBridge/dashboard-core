package org.literacybridge.dashboard.processes;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.hash.HashingInputStream;
import com.google.common.io.CountingInputStream;
import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang.mutable.MutableInt;
import org.literacybridge.dashboard.FullSyncher;
import org.literacybridge.dashboard.ProcessingResult;
import org.literacybridge.dashboard.model.syncOperations.UpdateProcessingState;
import org.literacybridge.dashboard.model.syncOperations.UsageUpdateRecord;
import org.literacybridge.dashboard.model.syncOperations.ValidationParameters;
import org.literacybridge.dashboard.services.S3Service;
import org.literacybridge.dashboard.services.SyncherService;
import org.literacybridge.dashboard.services.UpdateRecordWriterService;
import org.literacybridge.stats.DirectoryIterator;
import org.literacybridge.stats.model.DirectoryFormat;
import org.literacybridge.stats.model.validation.ValidationError;
import org.literacybridge.stats.processors.ValidatingProcessor;
import org.literacybridge.utils.FsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.commons.io.FileUtils.copyFile;
import static org.apache.commons.io.FileUtils.openOutputStream;

/**
 * Controls the lifecycle of content usage updates.
 */
@Service
public class ContentUsageUpdateProcess {

    static private final Logger logger = LoggerFactory.getLogger(ContentUsageUpdateProcess.class);

    @Autowired
    private S3Service s3Service;

    @Autowired
    private SyncherService syncherService;

    @Autowired
    private UpdateRecordWriterService updateRecordWriterService;

    public UpdateUsageContext createContext(UsageUpdateRecord processingState, File tempDir,
                                            FileCleaningTracker fileCleaningTracker,
                                            ProcessingResult result) {
        UpdateUsageContext context = new UpdateUsageContext(tempDir, fileCleaningTracker, result);
        context.setUpdateRecord(processingState);
        return context;
    }

    public UpdateUsageContext processUpdateUpload(InputStream is, File tempDir, String deviceName,
                                                  String updateName,
                                                  FileCleaningTracker fileCleaningTracker,
                                                  ProcessingResult result) throws IOException {

        //First create our empty context
        UpdateUsageContext context = new UpdateUsageContext(tempDir, fileCleaningTracker, result);

        //First put this down on temp storage
        File initialFile = context.createTempFile(TempFileType.initialFile);
        FileOutputStream fos = new FileOutputStream(initialFile);
        HashingInputStream shaIs = FsUtils.createSHAStream(is);
        CountingInputStream countingIs = FsUtils.createCountingStream(shaIs);

        try {
            IOUtils.copy(countingIs, fos);

            String sha256 = shaIs.hash().toString();
            UsageUpdateRecord updateRecord = createInitialUpdateRecord(sha256, deviceName,
                                                                       updateName);
            context.setUpdateRecord(updateRecord);
            return context;

        } finally {
            IOUtils.closeQuietly(fos);
            IOUtils.closeQuietly(countingIs);
        }
    }

    public UpdateUsageContext process(@Nonnull UpdateUsageContext context,
                                      ValidationParameters validationParameters) throws Exception {

        UpdateProcessingState state = context.getUpdateRecord().getState();
        while (state != UpdateProcessingState.done && state != UpdateProcessingState.failed) {
            context = processNextStep(context, validationParameters);
            state = context.getUpdateRecord().getState();
        }
        return context;
    }

    @Nonnull
    public UpdateUsageContext processNextStep(@Nonnull UpdateUsageContext context,
                                              ValidationParameters validationParameters)
            throws Exception {
        Preconditions.checkNotNull(context.getUpdateRecord().getState(),
                                   "UpdateRecord must be set and have a valid state.");

        switch (context.getUpdateRecord().getState()) {
        case initialized:
            context = validateAndUpload(context, validationParameters);
            break;

        case accepted:
            context = writeUpdatesToDb(context, validationParameters);
            break;

        case uploadedToDb:
            context = deleteTempFiles(context);
            //throw new NotYetImplementedException("Aggregation feature is not implemented yet");
            break;
        case done:
        case failed:
            //Nothing to do.
            break;
        }

        return context;
    }

    private UpdateUsageContext deleteTempFiles(@Nonnull UpdateUsageContext context) {

        File initialFile = context.tempFileMap.get(TempFileType.initialFile);
        File explodedDir = context.tempFileMap.get(TempFileType.explodedDir);
        initialFile.delete();
        try {
            FileUtils.deleteDirectory(explodedDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        context.getUpdateRecord().setState(UpdateProcessingState.done);
        return context;
    }

    /**
     * Validate that the zip file is valid, upload the file to S3, and write the status to the DB.
     *
     * After this call, the UpdateRecord should have a valid ID.
     *
     * @param context
     * @param validationParameters
     * @return
     * @throws Exception
     */
    public UpdateUsageContext validateAndUpload(@Nonnull UpdateUsageContext context,
                                                @Nonnull ValidationParameters validationParameters)
            throws Exception {

        final boolean SKIP_S3_UPLOAD = true;
        final UsageUpdateRecord updateRecord = context.getUpdateRecord();
        System.out.print("Checking S3 if already uploaded");
        final boolean s3ObjectAlreadyExists = SKIP_S3_UPLOAD ? true : s3Service.doesObjectExist(
                s3Service.getUploadBucket(), updateRecord.getS3Id());

        //If this has already been updated, then this code must have already been run.  In, this case, update the
        //state and roll.
        if (s3ObjectAlreadyExists) {
            System.out.println("...S3 object exists");
            //Check to see if there is an existing record, if there is, then use the existing one, and
            //update the state accordingly.  Namely, if this was an upload that caused an error, re-do it only if
            //the isForced flag is set.
            UsageUpdateRecord existingRecord = updateRecordWriterService.findByS3Id(
                    updateRecord.getS3Id());
            if (existingRecord != null) {
                context.setUpdateRecord(existingRecord);

                if (existingRecord.getState() == UpdateProcessingState.uploadedToDb
                        || existingRecord.getState() == UpdateProcessingState.aggegated
                        || existingRecord.getState() == UpdateProcessingState.done) {
                    System.out.println("...Already uploaded");
                    return context;
                }
            }
        }

        //Otherwise, first unzip and validate
        File initialFile = context.tempFileMap.get(TempFileType.initialFile);
        if (initialFile == null || !initialFile.exists()) {
            throw new IllegalArgumentException(
                    "Context state is invalid, because the initial download file does not exist!!!");
        }

        System.out.print("Unzipping");
        List<ValidationError> validationErrors = unzipAndValidate(context,
                                                                  validationParameters.getFormat(),
                                                                  validationParameters.isStrict());
        System.out.println("...Done unzipping");
        updateRecordBasedOnValidationErrors(updateRecord, validationErrors,
                                            validationParameters.isForce());
        context.validationErrors = validationErrors;

        if (!s3ObjectAlreadyExists) {
            System.out.print("Uploading to S3");
            uploadInitialFileTOS3(updateRecord, initialFile);
            System.out.println("...Done uploading to S3");
        }

        //
        //Now save out results
        updateRecordWriterService.writeWithErrors(updateRecord, validationErrors);
        return context;
    }

    @Nonnull
    private UsageUpdateRecord createInitialUpdateRecord(@Nonnull String sha256,
                                                        @Nonnull String deviceName,
                                                        @Nonnull String updateName) {

        UsageUpdateRecord updateRecord = new UsageUpdateRecord();
        updateRecord.setS3Id(sha256);
        updateRecord.setState(UpdateProcessingState.initialized);
        updateRecord.setStartTime(new Date());
        updateRecord.setExternalId(UUID.randomUUID().toString());
        updateRecord.setDeviceName(deviceName);
        updateRecord.setUpdateName(updateName);
        updateRecord.setSha256(sha256);

        return updateRecord;
    }

    @Nonnull
    private List<ValidationError> unzipAndValidate(@Nonnull UpdateUsageContext context,
                                                   @Nullable DirectoryFormat format,
                                                   boolean isStrict) throws Exception {

        File explodedDir = context.createTempFile(TempFileType.explodedDir);
        FsUtils.unzip(context.tempFileMap.get(TempFileType.initialFile), explodedDir);

/*    File[] f = explodedDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File pathname) {
        if (pathname.isDirectory() && !pathname.isHidden() && !pathname.getName().startsWith("_") && !pathname.getName().startsWith(".")) // avoid folders like __MACOSX
          return true;
        else
          return false;
      }
    });
    if (f.length != 1) {
      // should only be one non-hidden normal directory in the zip
      throw new Exception ("Bad directory structure inside zip.  Should include only one directory at top level.");
    }
    File root;
    if (f[0].getName().equalsIgnoreCase("collected-data")) {
      root = f[0];
    } else {
      // TODO: we should capture the name of the dropbox folder that this data came from and store it in the database.
      // This will help us analyze which laptop/tablet is getting the data.  Could tell us which staff person is gathering how much data.
      // But for now we will just pass it by and get to collected-data.
      root = new File(f[0],"collected-data");
      if (!root.exists()) {
        throw new Exception ("Bad directory structure inside zip.  There is no collected-data directory inside the top level directory.");
      }
    }
    DirectoryIterator iterator = new DirectoryIterator(root, format, isStrict);
*/
        DirectoryIterator iterator = new DirectoryIterator(explodedDir, format, isStrict,
                                                           context);
        ValidatingProcessor validatingProcessor = new ValidatingProcessor(context);
        iterator.process(validatingProcessor);
        return validatingProcessor.validationErrors;
    }

    private void updateRecordBasedOnValidationErrors(@Nonnull UsageUpdateRecord updateRecord,
                                                     @Nonnull List<ValidationError> validationErrors,
                                                     boolean force) {

        if (!validationErrors.isEmpty()) {
            if (!force) {
                updateRecord.setState(UpdateProcessingState.failed);
                updateRecord.setMessage(
                        "Failed with " + validationErrors.size() + ".  First error = "
                                + validationErrors.get(0).errorMessage);
            } else {
                updateRecord.setState(UpdateProcessingState.accepted);
                updateRecord.setMessage(validationErrors.size()
                                                + " validation erorrs.  Import proceeding, because 'force' option is set.  First error = "
                                                + validationErrors.get(0).errorMessage);
            }
        } else {
            updateRecord.setState(UpdateProcessingState.accepted);
            updateRecord.setMessage("File accepted with no validation errors.");
        }
    }

    private void uploadInitialFileTOS3(@Nonnull UsageUpdateRecord updateRecord, File initialFile)
            throws FileNotFoundException {
        Map<String, String> userMetadata = ImmutableMap.of("updateName",
                                                           updateRecord.getUpdateName(),
                                                           "deviceName",
                                                           updateRecord.getDeviceName(), "sha256",
                                                           updateRecord.getSha256());

        InputStream fis = new FileInputStream(initialFile);
        try {
            s3Service.writeZipObject(s3Service.getUploadBucket(), updateRecord.getS3Id(), fis,
                                     initialFile.length(), userMetadata);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    @Nonnull
    protected File assureExplodedDir(@Nonnull UpdateUsageContext context) throws IOException {
        File retVal = context.tempFileMap.get(TempFileType.explodedDir);
        if (retVal == null) {
            File initialFile = context.tempFileMap.get(TempFileType.initialFile);
            if (initialFile == null) {
                initialFile = context.createTempFile(TempFileType.initialFile);
                s3Service.getObject(s3Service.getUploadBucket(),
                                    context.getUpdateRecord().getS3Id(), initialFile);
            }

            retVal = context.createTempFile(TempFileType.explodedDir);
            FsUtils.unzip(initialFile, retVal);
        }

        return retVal;
    }

    public UpdateUsageContext writeUpdatesToDb(@Nonnull UpdateUsageContext context,
                                               @Nonnull ValidationParameters validationParameters)
            throws Exception {

        long start = System.currentTimeMillis() / 1000;
        File explodedDir = assureExplodedDir(context);

        FullSyncher fullSyncher = new FullSyncher(context.getUpdateRecord().getId(), .1,
                                                  Lists.newArrayList(syncherService.createSyncWriter()), context);
        fullSyncher.processData(explodedDir, validationParameters.getFormat(),
                                validationParameters.isStrict());
        fullSyncher.doConsistencyCheck();
        long end = System.currentTimeMillis() / 1000;
        context.getUpdateRecord().setState(UpdateProcessingState.uploadedToDb);
        updateRecordWriterService.write(context.getUpdateRecord());
        long elapsedSec = end - start;
        System.out.println("Time: " + elapsedSec + " seconds.");
        return context;
    }

    public UpdateUsageContext aggregateUpdates(@Nonnull UpdateUsageContext context,
                                               @Nonnull ValidationParameters validationParameters)
            throws Exception {

        File explodedDir = assureExplodedDir(context);

        FullSyncher fullSyncher = new FullSyncher(context.getUpdateRecord().getId().longValue(), .1,
                                                  Lists.newArrayList(syncherService.createSyncWriter()),
                                                  context);
        fullSyncher.processData(explodedDir);
        fullSyncher.doConsistencyCheck();

        context.getUpdateRecord().setState(UpdateProcessingState.uploadedToDb);
        updateRecordWriterService.write(context.getUpdateRecord());
        return context;
    }

    private File operationalDataLogsDir;
    private FileOutputStream tbDataLogs;
    private FileOutputStream deploymentsLogs;
    private FileOutputStream statsDataLogs;

    /**
     * Sets the (optional) directory into which logs from OperationalData are accumulated.
     * Once set, any tbData-2017y09m29d-000c.log, deploymetns-2017y09m29d-000c.log, and
     * statsData-2017y09m29d-000c.log files will be concatenated onto resulting
     * {d}/tbDataAll.log, {d}deploymentsAll.log, and {d}statsDataAll.log files.
     *
     * Any existing files are deleted first.
     *
     * @param d The directory. Must exist.
     */
    public void setOperationalLogDirectory(File d) {
        if (!d.exists() || !d.isDirectory()) {
            throw new IllegalStateException("OperationalLogDirectory must exist and be a directory.");
        }
        operationalDataLogsDir = d;
    }

    /**
     * Call to flush and close the accumulated operational data log files.
     * @throws IOException if any file can't be flushed or closed.
     */
    public void closeOperationalDirectory() throws IOException {
        if (tbDataLogs != null) {
            tbDataLogs.flush();
            tbDataLogs.close();
            tbDataLogs = null;
        }
        if (deploymentsLogs != null) {
            deploymentsLogs.flush();
            deploymentsLogs.close();
            deploymentsLogs = null;
        }
        if (statsDataLogs != null) {
            statsDataLogs.flush();
            statsDataLogs.close();
            statsDataLogs = null;
        }
        operationalDataLogsDir = null;
    }

    /**
     * Appends one "flavor" of operational data log files.
     * @param logsDir The directory containing the .log files.
     * @param filter to select only the desired files, by name.
     * @param accumulator an output stream to accumulate the log files.
     * @param count
     * @throws IOException if the file can't be written.
     */
    private FileOutputStream appendOperationalLog(
        File logsDir, Pattern filter, String fileName, FileOutputStream accumulator,
        MutableInt count) throws IOException {
        if (accumulator == null) {
            File f = new File(operationalDataLogsDir, fileName);
            if (f.exists()) f.delete();
            accumulator = openOutputStream(f);
        }
        File[] list = logsDir.listFiles((FilenameFilter) new RegexFileFilter(filter));
        if (list != null) {
            for (File f : list) {
                copyFile(f, accumulator);
                // Files should end with a new line, but in case they don't, add one.
                accumulator.write("\n".getBytes());
                count.increment();
            }
        }
        return accumulator;
    }
    private static final Pattern TB_DATA_LOGS = Pattern.compile(
        "tbData-(\\d+)y(\\d+)m(\\d+)d-(.*).log", Pattern.CASE_INSENSITIVE);
    private static final Pattern STATS_DATA_LOGS = Pattern.compile(
        "statsData-(\\d+)y(\\d+)m(\\d+)d-(.*).log", Pattern.CASE_INSENSITIVE);
    private static final Pattern DEPLOYMENTS_LOGS = Pattern.compile(
        "deployments-(\\d+)y(\\d+)m(\\d+)d-(.*).log", Pattern.CASE_INSENSITIVE);
    private static final String TB_DATA_LOG = "tbDataAll.log";
    private static final String STATS_DATA_LOG = "statsDataAll.log";
    private static final String DEPLOYMENTS_LOG = "deploymentsAll.log";
    /**
     * Given an OperationalData directory, append any contained .log files.
     * @param logsDir that may have data.
     * @throws IOException if any data can't be written.
     */
    private int appendOperationalLogsImpl(File logsDir) throws IOException {
        MutableInt count = new MutableInt(0);
        if (operationalDataLogsDir != null) {
            tbDataLogs = appendOperationalLog(logsDir, TB_DATA_LOGS, TB_DATA_LOG, tbDataLogs, count);
            statsDataLogs = appendOperationalLog(logsDir, STATS_DATA_LOGS, STATS_DATA_LOG, statsDataLogs, count);
            deploymentsLogs = appendOperationalLog(logsDir, DEPLOYMENTS_LOGS, DEPLOYMENTS_LOG, deploymentsLogs, count);
        }
        return count.intValue();
    }

    /**
     * Unique definitions for the different temp file types.
     */
    enum TempFileType {
        initialFile(".zip", false),
        explodedDir("", true),
        aggregationDest("", true),
        zippedAggregation(".zip", false);

        public final String suffix;
        public final boolean isDir;

        TempFileType(String suffix, boolean dir) {
            this.suffix = suffix;
            isDir = dir;
        }
    }

    /**
     * Contains the full context of an update usage update.  This references the canonical
     * state information that will go in the DB as well as the in flight transient references to things
     * like temp files and directories.
     *
     * This structure is responsible for allocating and tracking temporary files in such a way that they will be cleaned
     * up by a tracker thread.
     */
    public class UpdateUsageContext {

        public final File tempDirRoot;
        public final FileCleaningTracker fileCleaningTracker;
        public final Map<TempFileType, File> tempFileMap = new HashMap<>();

        public List<ValidationError> validationErrors = Collections.EMPTY_LIST;

        public ProcessingResult result;

        protected UsageUpdateRecord updateRecord;

        public UpdateUsageContext(File tempDirRoot, FileCleaningTracker fileCleaningTracker,
                                  ProcessingResult result) {
            this.tempDirRoot = tempDirRoot;
            this.fileCleaningTracker = fileCleaningTracker;
            this.result = result;
        }

        public UsageUpdateRecord getUpdateRecord() {
            return updateRecord;
        }

        public void setUpdateRecord(UsageUpdateRecord updateRecord) {
            this.updateRecord = updateRecord;
        }

        public File createTempFile(TempFileType type) throws IOException {
            final File retVal = File.createTempFile(type.toString(), type.suffix, tempDirRoot);
            if (type.isDir) {
                retVal.delete();
                retVal.mkdir();
            }

            if (fileCleaningTracker != null) {
                fileCleaningTracker.track(retVal, retVal, FileDeleteStrategy.FORCE);
            }
            tempFileMap.put(type, retVal);

            return retVal;
        }

        public boolean deleteTempFile(TempFileType type) {
            File tempFile = tempFileMap.remove(type);
            if (tempFile != null) {
                return FileUtils.deleteQuietly(tempFile);
            }

            return true;
        }

        public int appendOperationalLogs(File logsDir) throws IOException {
            // Defer to containing object.
            return appendOperationalLogsImpl(logsDir);
        }
    }

}
