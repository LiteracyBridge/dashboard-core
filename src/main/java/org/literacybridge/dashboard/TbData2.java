package org.literacybridge.dashboard;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.StringUtils;
import org.literacybridge.dashboard.model.syncOperations.UpdateProcessingState;
import org.literacybridge.dashboard.model.syncOperations.ValidationParameters;
import org.literacybridge.dashboard.processes.ContentUsageUpdateProcess;
import org.literacybridge.stats.model.DirectoryFormat;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.*;

/**
 * Wrapper class to translate commandline options into the appropriate service calls.  It is important to note that this
 * class talks directly to the DB, NOT through REST APIs on the dashboard server.  Someone SHOULD write a commandline to
 * talk to REST interfaces, however, that doesn't exist yet. . .
 */
public class TbData2 {

  // create Options object
  public static final Options options = new Options();

  static {
    options.addOption("?", false, "Help");
    options.addOption("z", true, "Zip file to process.");
    options.addOption("e", true, "file to write validation errors to.  If false, they will be written to stdout.");

    options.addOption("o", false, "Directory format is using the older format");
    options.addOption("f", false, "Force update, even if there are errors.");
    options.addOption("s", false, "Do strict format checks.");

  }

  public static ContentUsageUpdateProcess.UpdateUsageContext importZip(ContentUsageUpdateProcess contentUsageUpdateProcess, File zipFile, ValidationParameters validationParameters) throws Exception {
    InputStream is = FileUtils.openInputStream(zipFile);
    File tempDir = new File(System.getProperty("java.io.tmpdir"));
    ContentUsageUpdateProcess.UpdateUsageContext context = contentUsageUpdateProcess.processUpdateUpload(is, tempDir, "Commandline Tool", "Non Specific", null);
    context = contentUsageUpdateProcess.process(context, validationParameters);

    return context;
  }


  public static void main(String[] args) throws Exception {
    ApplicationContext applicationContext = new ClassPathXmlApplicationContext(
        "spring/lb-core-spring.xml");

    ContentUsageUpdateProcess contentUsageUpdateProcess = (ContentUsageUpdateProcess) applicationContext.getBean("contentUsageUpdateProcess");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse( options, args);

    if (cmd.hasOption("?")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "tbdata", options );
      return;
    }


    ValidationParameters validationParameters = new ValidationParameters();
    validationParameters.setForce(cmd.hasOption("f"));
    validationParameters.setStrict(cmd.hasOption("s"));
    validationParameters.setFormat(cmd.hasOption("o") ? DirectoryFormat.Sync : DirectoryFormat.Archive);

    if (!cmd.hasOption("z")) {
      System.out.println("ERROR:  MUST provide the -z option for the zip file to import.");

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "tbdata", options );
      return;
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
      filesToProcess = new File[]{zipFile};
    }

    for (File fileToProcess : filesToProcess) {
      System.out.print("Importing " + fileToProcess.getName() + "...");
      ContentUsageUpdateProcess.UpdateUsageContext context = importZip(contentUsageUpdateProcess, fileToProcess, validationParameters);

      System.out.println(((context.getUpdateRecord().getState() == UpdateProcessingState.failed) ? "FAILED" : "SUCCEEDED"));
      System.out.println("Imported the process with ID=" + context.getUpdateRecord().getExternalId());
      if (!context.validationErrors.isEmpty()) {
        errorOut.write(("Validation Errors:\n" + StringUtils.join(context.validationErrors, "\n")).getBytes());
      }
    }

    errorOut.flush();
    errorOut.close();
  }
}
