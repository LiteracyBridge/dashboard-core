package org.literacybridge.dashboard;
import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.literacybridge.dashboard.aggregation.StatAggregator;
import org.literacybridge.dashboard.model.syncOperations.UpdateProcessingState;
import org.literacybridge.dashboard.model.syncOperations.ValidationParameters;
import org.literacybridge.dashboard.processes.ContentUsageUpdateProcess;
import org.literacybridge.stats.DataArchiver;
import org.literacybridge.dashboard.services.SyncherService;
import org.literacybridge.dashboard.services.UpdateRecordWriterService;
import org.literacybridge.stats.model.DirectoryFormat;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.InputStream;

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

    options.addOption("o", false, "Directory format is using the older format");
    options.addOption("f", false, "Force update, even if there are errors.");
    options.addOption("s", false, "Do strict format checks.");

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


    File zipFile = new File (cmd.getOptionValue("z", "."));
    InputStream is = FileUtils.openInputStream(zipFile);
    File  tempDir = new File(System.getProperty("java.io.tmpdir"));
    ContentUsageUpdateProcess.UpdateUsageContext context = contentUsageUpdateProcess.processUpdateUpload(is,tempDir, "Commandline Tool", "Non Specific", null);
    context = contentUsageUpdateProcess.process(context, validationParameters);

    System.out.println("Imported " + ((context.getUpdateRecord().getState() == UpdateProcessingState.failed) ? "FAILED" : "SUCCEEDED"));
    System.out.println("Imported the process with ID=" + context.getUpdateRecord().getExternalId());
    if (!context.validationErrors.isEmpty()) {
      System.out.println("Validation Errors:\n" + StringUtils.join(context.validationErrors, "\n"));
    }

  }
}
