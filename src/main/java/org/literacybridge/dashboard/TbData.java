package org.literacybridge.dashboard;

import com.google.common.collect.Lists;
import org.apache.commons.cli.*;
import org.literacybridge.dashboard.aggregation.StatAggregator;
import org.literacybridge.stats.DataArchiver;
import org.literacybridge.dashboard.services.SyncherService;
import org.literacybridge.dashboard.services.UpdateRecordWriterService;
import org.literacybridge.stats.model.DirectoryFormat;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;

/**
 * Wrapper class to translate commandline options into the appropriate service calls.  It is important to note that this
 * class talks directly to the DB, NOT through REST APIs on the dashboard server.  Someone SHOULD write a commandline to
 * talk to REST interfaces, however, that doesn't exist yet. . .
 */
public class TbData {

  // create Options object
  public static final Options options = new Options();

  static {
    options.addOption("?", false, "Help");
    options.addOption("r", true,
                      "Root directory to process.  This will look for all sub-directories that look like \n" +
                          "they contain a device's data sync directory.  The device's sync directories are recognized \n" +
                          "by the collected-data sub directory.");

    options.addOption("d", true,
                      "Device sync directory to process.  This processes the content update directories under the \n" +
                          "collected-data sub directory of the laptop dir.");
    options.addOption("D", true,
                      "Name of the device this sync occured from.  This option should NOT be used with the -d option, \n" +
                          "and is questionable to use with -l.");

    options.addOption("u", true,
                      "Content update directory to process.  This processes the village directories under this directory.");
    options.addOption("U", true,
                      "Name of the content udpate this sync is from.  This option should NOT be used with the -d option.");

    options.addOption("v", true,
                      "Village sync directory to process.  This will process all the talking books for a village.  By \n" +
                          "default, this will look to the parent directories to find the laptop the sync occurred from, \n" +
                          "to override this use the -L option.  If this info is in a FlashData.bin file, that data will \n" +
                          "be considered authoritative.");
    options.addOption("V", true,
                      "Name of the village this sync is occuring with.  Should not be used with -l, -d or -v options.");

    options.addOption("t", true,
                      "TalkingBook sync directory to process.  This will processes the sync instances that occured \n" +
                          "under the specified TalkingBook directory.  Village and laptop are determined from the file \n" +
                          "system. To override, use -L or -V");
    options.addOption("T", true,
                      "Name of the TalkingBook this process is occurring with.  Should not be used with -l, -d or -v options.");

    options.addOption("a", false, "Directory format is using the 'archive' format");
    options.addOption("c", true,  "Copy the structure to the argument provided.  If this is used with -a, the result will be \n" +
                                  "in the archive format.");
    options.addOption("C", true, "When doing a copy (-c) correct any corruption that is seen.");

    options.addOption("f", true, "Run the fix-up processing on the supplied directory.");

  }


  public static void main(String[] args) throws Exception {
    ApplicationContext context = new ClassPathXmlApplicationContext(
        "spring/lb-core-spring.xml");

    SyncherService syncherService = (SyncherService) context.getBean("syncherService");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse( options, args);

    if (cmd.hasOption("?")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( "tbdata", options );
      return;
    }


    //UNDONE(willpugh) -- Fix me!!  Does not create a proper update record
    FullSyncher fullSyncher = new FullSyncher(0, .1, Lists.newArrayList(syncherService.createSyncWriter()));

    if (cmd.hasOption("d")) {
      File    deviceSyncDir = new File(cmd.getOptionValue("d"));
      File    rootDir =  deviceSyncDir.getParentFile();
      fullSyncher.processDeviceDir(deviceSyncDir.getName(), rootDir);
    } else if (cmd.hasOption("u")) {
      File    updateSyncDir = new File(cmd.getOptionValue("u"));
      File    rootDir =  updateSyncDir.getParentFile().getParentFile();
      String  deviceId = cmd.getOptionValue("D", updateSyncDir.getParentFile().getParent());
      fullSyncher.processUpdateDir(deviceId, updateSyncDir.getName(), rootDir);
    } else if (cmd.hasOption("v")) {
      File    villageSyncDir = new File(cmd.getOptionValue("v"));
      File    rootDir =  villageSyncDir.getParentFile().getParentFile().getParentFile().getParentFile();
      String  deviceId = cmd.getOptionValue("D", villageSyncDir.getParentFile().getParentFile().getParent());
      String  updateId = cmd.getOptionValue("U", villageSyncDir.getParent());
      fullSyncher.processVillageDir(deviceId, villageSyncDir.getName(), updateId, rootDir);
    } else if (cmd.hasOption("t")) {
      File    talkingBookSyncDir = new File(cmd.getOptionValue("t"));
      File    rootDir =  talkingBookSyncDir.getParentFile().getParentFile().getParentFile().getParentFile().getParentFile();
      String  deviceId  = cmd.getOptionValue("D", talkingBookSyncDir.getParentFile().getParentFile().getParentFile().getParent());
      String  updateId  = cmd.getOptionValue("U", talkingBookSyncDir.getParentFile().getParent());
      String  villageId = cmd.getOptionValue("V", talkingBookSyncDir.getParent());
      fullSyncher.processTalkingBookDir(deviceId, talkingBookSyncDir.getName(), updateId, villageId, rootDir);
    } else if (cmd.hasOption("c")) {
      File  destDir = new File(cmd.getOptionValue("c"));
      File  srcDir = new File (cmd.getOptionValue("r", "."));
      DataArchiver  dataArchiver = new DataArchiver(destDir, DirectoryFormat.Sync, srcDir, DirectoryFormat.Sync, false);
      dataArchiver.archive();
    } else if (cmd.hasOption("C")) {
      File  destDir = new File(cmd.getOptionValue("C"));
      File  srcDir = new File (cmd.getOptionValue("r", "."));
      DataArchiver  dataArchiver = new DataArchiver(destDir, DirectoryFormat.Sync, srcDir, DirectoryFormat.Sync, true);
      dataArchiver.archive();
    } else if (cmd.hasOption("f")) {
      File  destDir = new File(cmd.getOptionValue("f"));
      DataArchiver.fixupOnly(destDir, cmd.hasOption("a") ? DirectoryFormat.Archive : DirectoryFormat.Sync);
    } else {
      File root = new File (cmd.getOptionValue("r", "."));
      fullSyncher.processData(root, cmd.hasOption("a") ? DirectoryFormat.Archive : DirectoryFormat.Sync, false);
    }

    StatAggregator stats = fullSyncher.doConsistencyCheck();
    System.out.println(stats.perUpdateAggregations.size());
  }
}
