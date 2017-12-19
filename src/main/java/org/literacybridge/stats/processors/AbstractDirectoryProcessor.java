package org.literacybridge.stats.processors;

import org.literacybridge.dashboard.ProcessingResult;
import org.literacybridge.dashboard.processes.ContentUsageUpdateProcess;
import org.literacybridge.stats.api.DirectoryCallbacks;
import org.literacybridge.stats.model.*;

import java.io.File;
import java.io.IOException;

/**
 */
abstract public class AbstractDirectoryProcessor implements DirectoryCallbacks {
  protected ProcessingResult result;
  ContentUsageUpdateProcess.UpdateUsageContext context;

  protected File currRoot;
  protected DirectoryFormat format;
  protected StatsPackageManifest manifest;
  protected String currDevice;
  protected DeploymentPerDevice currDeploymentPerDevice;
  protected DeploymentId deploymentId;
  protected String currVillage;
  protected String currTalkingBook;

  protected AbstractDirectoryProcessor(ContentUsageUpdateProcess.UpdateUsageContext context) {
      this.result = context.result;
      this.context = context;
  }

  @Override
  public boolean startProcessing(File root, StatsPackageManifest manifest, DirectoryFormat format) throws Exception {
    this.currRoot = root;
    this.format = format;
    this.manifest = manifest;
    result.addProject(root.getName());
    return true;
  }

  @Override
  public void endProcessing() throws Exception {
    currRoot = null;
  }


  @Override
  public boolean startDeviceOperationalData(String device) {
      currDevice = device;
      return false;
  }

  @Override
  public void endDeviceOperationalData() {
      currDevice = null;
  }

  @Override
  public void processTbDataFile(File tbdataFile, boolean includesHeaders) throws IOException {

  }

  @Override
  public void processTbLoaderLogFile(File logFile) throws IOException {

  }

  @Override
  public boolean startDeviceAndDeployment(DeploymentPerDevice deploymentPerDevice) throws Exception {
      result.addDeployment(currRoot.getName(), deploymentPerDevice.device, deploymentPerDevice.deployment);
    currDeploymentPerDevice = deploymentPerDevice;
    this.deploymentId = deploymentId;
    return true;
  }

  @Override
  public void endDeviceAndDeployment() throws Exception {
    currDeploymentPerDevice = null;
    deploymentId = null;
  }

  @Override
  public boolean startVillage(String village) throws Exception {
      result.addVillage(currRoot.getName(), currDeploymentPerDevice.device, currDeploymentPerDevice.deployment, village);
    currVillage = village;
    return true;
  }

  @Override
  public void endVillage() throws Exception {
    currVillage = null;
  }

  @Override
  public boolean startTalkingBook(String talkingBook) throws Exception {
      result.addTalkingBook(currRoot.getName(), currDeploymentPerDevice.device, currDeploymentPerDevice.deployment, currVillage, talkingBook);
    currTalkingBook = talkingBook;
    return true;
  }

  @Override
  public void endTalkingBook() {
    currTalkingBook = null;
  }

  @Override
  public void processSyncDir(SyncDirId syncDirId, File syncDir) throws Exception {
  }
}
