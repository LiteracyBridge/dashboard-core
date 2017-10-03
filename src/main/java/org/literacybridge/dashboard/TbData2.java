package org.literacybridge.dashboard;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.StringUtils;
import org.literacybridge.dashboard.model.syncOperations.UpdateProcessingState;
import org.literacybridge.dashboard.model.syncOperations.ValidationParameters;
import org.literacybridge.dashboard.processes.ContentUsageUpdateProcess;
import org.literacybridge.stats.model.DirectoryFormat;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

/**
 * Wrapper class to translate commandline options into the appropriate service calls.  It is important to note that this
 * class talks directly to the DB, NOT through REST APIs on the dashboard server.  Someone SHOULD write a commandline to
 * talk to REST interfaces, however, that doesn't exist yet. . .
 */
public class TbData2 {
    /**
     * Turn down the Hibernate and Spring noise...
     * Despite what you may find on the internet, the Hibernate and Spring logging, at
     * least the version we're using, log directly to java.util.logging. So, we use
     * java.util.logging to control it.
     */
    static {
        java.util.logging.Logger logger = java.util.logging.Logger.getGlobal();
        logger.setLevel(java.util.logging.Level.WARNING);
    }

    // create Options object
    public static final Options options = new Options();

    static {
        options.addOption("?", false, "Help");
        options.addOption("z", true, "Zip file to process.");
        options.addOption("e", true,
            "file to write validation errors to.  If false, they will be written to stdout.");
        options.addOption("r", true,
            "File to receive a report. If named '*.html', will be formatted HTML.");
        options.addOption("d", true,
            "Directory into which to accumulate OperationalData .log files.");

        options.addOption("o", false, "Directory format is using the older format");
        options.addOption("f", false, "Force update, even if there are errors.");
        options.addOption("s", false, "Do strict format checks.");

    }

    public static ContentUsageUpdateProcess.UpdateUsageContext importZip(
        ContentUsageUpdateProcess contentUsageUpdateProcess, File zipFile,
        ValidationParameters validationParameters,
        ProcessingResult result) throws Exception {
        InputStream is = FileUtils.openInputStream(zipFile);
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        ContentUsageUpdateProcess.UpdateUsageContext context = contentUsageUpdateProcess.processUpdateUpload(
            is, tempDir, "Commandline Tool", "Non Specific", null, result);
        context = contentUsageUpdateProcess.process(context, validationParameters);

        return context;
    }

    public static void main(String[] args) throws Exception {
        new TbData2().go(args);
    }

    private void go(String[] args) throws Exception {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext(
            "spring/lb-core-spring.xml");

        Properties buildProps = new Properties();
        buildProps.load(getClass().getResourceAsStream("/version.properties"));
        System.out.printf("Stats Importer version %s, built on %s\n",
            buildProps.getProperty("version"), buildProps.getProperty("build.date"));

        ContentUsageUpdateProcess contentUsageUpdateProcess = (ContentUsageUpdateProcess) applicationContext
            .getBean("contentUsageUpdateProcess");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("?")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("tbdata", options);
            return;
        }

        ValidationParameters validationParameters = new ValidationParameters();
        validationParameters.setForce(cmd.hasOption("f"));
        validationParameters.setStrict(cmd.hasOption("s"));
        validationParameters.setFormat(
            cmd.hasOption("o") ? DirectoryFormat.Sync : DirectoryFormat.Archive);

        if (!cmd.hasOption("z")) {
            System.out.println("ERROR:  MUST provide the -z option for the zip file to import.");

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("tbdata", options);
            return;
        }

        if (cmd.hasOption("d")) {
            contentUsageUpdateProcess.setOperationalLogDirectory(
                new File(cmd.getOptionValue("d", ".")));
        }

        final File zipFile = new File(cmd.getOptionValue("z", "."));
        File[] filesToProcess;

        final String errorFile = cmd.getOptionValue("e");
        OutputStream errorOut = System.out;
        if (errorFile != null) {
            errorOut = new FileOutputStream(errorFile);
        }

        if (zipFile.isDirectory()) {
            filesToProcess = zipFile.listFiles((FilenameFilter) new SuffixFileFilter("zip"));
        } else {
            filesToProcess = new File[] { zipFile };
        }

        for (File fileToProcess : filesToProcess) {
            ProcessingResult result = new ProcessingResult(zipFile.getName(),
                fileToProcess.getName());
            System.out.println("\nImporting " + fileToProcess.getName() + "...");
            ContentUsageUpdateProcess.UpdateUsageContext context = importZip(
                contentUsageUpdateProcess, fileToProcess, validationParameters, result);

            System.out.println(
                ((context.getUpdateRecord().getState() == UpdateProcessingState.failed) ?
                 "FAILED" :
                 "SUCCEEDED"));
            System.out.println(
                "Imported the process with ID=" + context.getUpdateRecord().getExternalId());
            if (!context.validationErrors.isEmpty()) {
                errorOut.write(("Validation Errors:\n" + StringUtils.join(context.validationErrors,
                    "\n")).getBytes());
            }
            if (cmd.hasOption("r")) {
                result.report(new File(cmd.getOptionValue("r")));
            } else {
                result.report(System.out);
            }
        }

        errorOut.flush();
        errorOut.close();
    }
}
