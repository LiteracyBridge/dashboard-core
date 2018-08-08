package org.literacybridge.main;

import java.util.Arrays;
import java.util.List;

/**
 * A class to accumulate the results of importing statistics. Created by bill on 4/26/17.
 */
public class ProcessingResult extends ResultTree {

    public ProcessingResult(String containingDirectory, String zipFileName) {
        super(containingDirectory);
        addAttribute("Zip File", zipFileName);
    }

    @Override
    protected String reportClass() {
        return "import-stats";
    }

    /**
     * Adds a Project to set of projects that have been seen.
     *
     * @param project The project.
     */
    public void addProject(String project) {
        addChild(project);
    }

    /**
     * Records that the project had no TalkingBookData.
     * @param project The project.
     * @param parent The directory that is missing a subdirectory.
     * @param directory The missing subdirectory.
     */
    public void addProjectHasMissingDirectory(String project,
        String parent,
        String directory) {
        addAttribute(Arrays.asList(project), "MissingDirectory", parent + '/' + directory);
     }

    /**
     * Adds a deployment for a project. Note that the project is the project in whose
     * context the TBLoader operation was performed. Any Talking Books that had been
     * previously part of a different project will have statistics for that other project.
     * Sadly, those Talking Books may not know what project that was, as the addition of
     * the project to the Talking Book occurred late in 2016.
     *
     * @param project    The project containing the deployment.
     * @param tbLoaderId The tbloader device that performed the TB update operation.
     * @param deployment The deployment.
     */
    public void addDeployment(String project, String tbLoaderId, String deployment) {
        addChild(Arrays.asList(project, tbLoaderId, deployment));
    }

    /**
     * Add a village for a project and deployment.
     *
     * @param project    The project containing the village.
     * @param tbLoaderId The tbloader device that performed the TB update operation.
     * @param deployment The deployment for which the village was visited.
     * @param village    The village.
     */
    public void addVillage(String project, String tbLoaderId, String deployment, String village) {
        addChild(Arrays.asList(project, tbLoaderId, deployment, village));
    }

    /**
     * Add a Talking Book for a project, deployment, village.
     *
     * @param project     The project containing the village.
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The deployment for which the village was visited.
     * @param village     The village where the Talking Book was updated.
     * @param talkingBook The Talking Book
     */
    public void addTalkingBook(String project, String tbLoaderId, String deployment, String village,
                               String talkingBook) {
        addChild(Arrays.asList(project, tbLoaderId, deployment, village, talkingBook));
    }

    /**
     * Record an unexpected 'Action' value from a .csv file in operationdata
     *
     * @param project        The project being processed.
     * @param tbLoaderId     The tbloader device that performed the TB update operation.
     * @param opDataFileName The file name with the bad action
     * @param action         The bad action
     */
    public void addUnexpectedOperationalAction(String project, String tbLoaderId, String opDataFileName,
                                               String action) {
        addAttribute(Arrays.asList(project, tbLoaderId, opDataFileName), "UnexpectedOperationalAction", action);
    }

    /**
     * Record a corrupted line in an operation log. If the operation log has multiple bad lines,
     * it is still only recorded once.
     *
     * @param project        The project containing the village.
     * @param tbLoaderId     The tbloader device that performed the TB update operation.
     * @param opDataFileName The file name with the corrupt line
     */
    public void addCorruptOperationLine(String project, String tbLoaderId, String opDataFileName) {
        addAttribute(Arrays.asList(project, tbLoaderId), "CorruptOperationalLine", opDataFileName);
    }

    /**
     * Record a corrupt, unreadable, or missing statistics directory. This directory should have been
     * contained in the Talking Book .zip file. Expected path is like this:
     * tbcd1234 / collected-data / PROJECT / talkingbookdata / DEPLOYMENT / 1234 / VILLAGE /
     * TALKINGBOOK / SYNCDIR(2017y04m26d...-1234) / statistics
     *
     * @param project     The project containing the village.
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The deployment for which the village was visited.
     * @param village     The village where the Talking Book was updated.
     * @param talkingBook The Talking Book with the missing statistics directory.
     * @param syncDirName The directory that should have contained a "statistics" directory.
     */
    public void addCorruptStatisticsDir(String project, String tbLoaderId, String deployment,
                                        String village, String talkingBook, String syncDirName) {
        addAttribute(
                Arrays.asList(project, tbLoaderId, deployment, village, talkingBook),
                "CorruptStatisticsDir", syncDirName);
    }

    /**
     * Record a corrupt or unreadable Talking Book zip file. The zip file would have a name like
     * tbcd1234 / collected-data / PROJECT / talkingbookdata / DEPLOYMENT / 1234 / VILLAGE /
     * TALKINGBOOK / SYNCDIR(2017y04m26d...-1234).zip
     *
     * @param project     The project containing the village.
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The deployment for which the village was visited.
     * @param village     The village where the Talking Book was updated.
     * @param talkingBook The Talking Book with the missing statistics directory.
     * @param zipFileName The file that should have been like "2017y07m07d14h23m58s-001D.zip", in
     *                    a "B-000C1234" Talking Book directory.
     */
    public void addCorruptedTalkingBookZip(String project, String tbLoaderId, String deployment,
                                           String village, String talkingBook, String zipFileName) {
        addAttribute(
                Arrays.asList(project, tbLoaderId, deployment, village, talkingBook),
                "CorruptTBZip", zipFileName);
    }

    /**
     * Records that there is a "sync dir" (a zip file under a TB directory) that is not mentioned
     * in the TBData file.
     *
     * @param project     The project containing the village.
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The deployment for which the village was visited.
     * @param village     The village where the Talking Book was updated.
     * @param talkingBook The Talking Book with the missing statistics directory.
     * @param syncDirName The directory that should have contained a "statistics" directory.
     */
    public void addSyncDirButNoOperationalData(String project, String tbLoaderId, String deployment,
                                               String village, String talkingBook,
                                               String syncDirName) {
        addAttribute(
                Arrays.asList(project, tbLoaderId, deployment, village, talkingBook),
                "SyncDirNoOpData", syncDirName);
    }

    /**
     * Records that there is a matchmatch between one of the "in-" values in the operational
     * data .csv file, and the values from the actual sync data. (Some of the values are
     * directory names.)
     *
     * @param project     The project containing the village.
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The deployment for which the village was visited.
     * @param village     The village where the Talking Book was updated.
     * @param talkingBook The Talking Book with the missing statistics directory.
     * @param syncDirName The directory that should have contained a "statistics" directory.
     */
    public void addIncorrectPropertyValues(String project, String tbLoaderId, String deployment,
                                           String village, String talkingBook, String syncDirName) {
        addAttribute(
                Arrays.asList(project, tbLoaderId, deployment, village, talkingBook),
                "IncorrectPropValue", syncDirName);
    }

    /**
     * This records that there exists operationalData .csv entry for which there is no
     * corresponding talkingbookdata syncdir. The values reported are from the operationaldata
     * (of course -- there is no sync dir from which to get other values)
     *
     * @param project     The project
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The outDeployment from the op data
     * @param village     The outVillage from the op data
     * @param talkingBook The outTalkingBook from the opData
     * @param syncDirName The syncdirname from the opData
     */
    public void addOperationalDataButNoSyncDir(String project, String tbLoaderId, String deployment,
                                               String village, String talkingBook,
                                               String syncDirName) {
        addAttribute(
                Arrays.asList(project, tbLoaderId, deployment, village, talkingBook),
                "OpDataNoSyncDir", syncDirName);
    }

    /**
     * This records the count of log files from a single Talking Book.
     *
     * @param project     The project
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The outDeployment from the op data
     * @param village     The outVillage from the op data
     * @param talkingBook The outTalkingBook from the opData
     * @param numLogFiles The number of log files
     */
    public void addCountLogFiles(String project, String tbLoaderId, String deployment,
                                 String village, String talkingBook, Integer numLogFiles) {
        addAttribute(
                Arrays.asList(project, tbLoaderId, deployment, village, talkingBook), "NumLogFiles",
                numLogFiles.toString());
    }

    /**
     * This records the count of errors in log files, from a single Talking Book.
     *
     * @param project     The project
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The outDeployment from the op data
     * @param village     The outVillage from the op data
     * @param talkingBook The outTalkingBook from the opData
     * @param numLogFileErrors The number of errors in the Talking Book's log files
     */
    public void addCountLogFileErrors(String project, String tbLoaderId, String deployment,
                                      String village, String talkingBook, Integer numLogFileErrors) {
        if (numLogFileErrors == 0) return;
        addAttribute(
                Arrays.asList(project, tbLoaderId, deployment, village, talkingBook), "NumLogFileErrors",
                numLogFileErrors.toString());
    }

    /**
     * This records the count of log files with errors, from a single Talking Book.
     *
     * @param project     The project
     * @param tbLoaderId  The tbloader device that performed the TB update operation.
     * @param deployment  The outDeployment from the op data
     * @param village     The outVillage from the op data
     * @param talkingBook The outTalkingBook from the opData
     * @param numLogFilesWithErrors The number of log files with errors from this Talking BOok
     */
    public void addCountLogFilesWithErrors(String project, String tbLoaderId, String deployment,
                                           String village, String talkingBook,
                                           Integer numLogFilesWithErrors) {
        if (numLogFilesWithErrors == 0) return;
        addAttribute(
                Arrays.asList(project, tbLoaderId, deployment, village, talkingBook), "NumLogFilesWithErrors",
                numLogFilesWithErrors.toString());
    }

    public void addOperationaLogError(String project, String device) {
        addAttribute(
            Arrays.asList(project, device), "OperationaLogError", project);
    }

    public void addOperationalLogsAppended(String project, String device, Integer count) {
        Integer newCount = new Integer(count);
        List<String> path = Arrays.asList(project, device);
        String countStr = getAttribute(path, "OperationaLogsAppended");
        if (countStr != null) {
            newCount += Integer.parseInt(countStr);
        }
        addAttribute(path, "OperationaLogsAppended", newCount.toString());
    }

}
