package org.literacybridge.stats.processors;

import org.literacybridge.stats.api.DirectoryCallbacks;
import org.literacybridge.stats.model.DeploymentPerDevice;
import org.literacybridge.stats.model.DirectoryFormat;
import org.literacybridge.stats.model.StatsPackageManifest;
import org.literacybridge.stats.model.SyncDirId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

/**
 * A processor that can wrap another processor and filter out the talking book syncs that will be processed.  The
 * TBData processing can only be filtered at a per-device level.
 */
public class FilteringProcessor implements DirectoryCallbacks {
  public final DirectoryCallbacks callbacks;
  public final String allowedDevice;
  public final String allowedContentUpdate;
  public final String allowedVillage;
  public final String allowedTalkingBook;

  public FilteringProcessor(@Nonnull DirectoryCallbacks callbacks) {
    this(callbacks, null, null, null, null);
  }

  public FilteringProcessor(@Nonnull DirectoryCallbacks callbacks, @Nullable String allowedDevice,
                            @Nullable String allowedContentUpdate, @Nullable String allowedVillage,
                            @Nullable String allowedTalkingBook) {
    this.callbacks = callbacks;
    this.allowedDevice = allowedDevice;
    this.allowedContentUpdate = allowedContentUpdate;
    this.allowedVillage = allowedVillage;
    this.allowedTalkingBook = allowedTalkingBook;
  }

  @Override
  public boolean startProcessing(File root, StatsPackageManifest manifest, DirectoryFormat format) throws Exception {
    return callbacks.startProcessing(root, manifest, format);
  }

  @Override
  public void endProcessing() throws Exception {
    callbacks.endProcessing();
  }

  @Override
  public boolean startDeviceOperationalData(String device) {
    if (allowedDevice != null && !allowedDevice.equalsIgnoreCase(device)) {
      return false;
    }
    return callbacks.startDeviceOperationalData(device);
  }

  @Override
  public void endDeviceOperationalData() {
    callbacks.endDeviceOperationalData();
  }

  @Override
  public void processTbDataFile(File tbdataFile, boolean includesHeaders) throws IOException {
    callbacks.processTbDataFile(tbdataFile, includesHeaders);
  }

  @Override
  public void processTbLoaderLogFile(File logFile) throws IOException {
    callbacks.processTbLoaderLogFile(logFile);
  }

  @Override
  public boolean startDeviceAndDeployment(DeploymentPerDevice deploymentPerDevice)
    throws Exception {

    if (allowedDevice != null && !allowedDevice.equalsIgnoreCase(deploymentPerDevice.device)) {
      return false;
    }

    if (allowedContentUpdate != null && !allowedContentUpdate.equalsIgnoreCase(deploymentPerDevice.deployment)) {
      return false;
    }

    return callbacks.startDeviceAndDeployment(deploymentPerDevice);
  }

  @Override
  public void endDeviceAndDeployment() throws Exception {
    callbacks.endDeviceAndDeployment();
  }

  @Override
  public boolean startVillage(String village) throws Exception {
    if (allowedVillage != null && !allowedVillage.equalsIgnoreCase(village)) {
      return false;
    }
    return callbacks.startVillage(village);
  }

  @Override
  public void endVillage() throws Exception {
    callbacks.endVillage();
  }

  @Override
  public boolean startTalkingBook(String talkingBook) throws Exception {
    if (allowedTalkingBook != null && !allowedTalkingBook.equalsIgnoreCase(talkingBook)) {
      return false;
    }
    return callbacks.startTalkingBook(talkingBook);
  }

  @Override
  public void endTalkingBook() {
    callbacks.endTalkingBook();
  }

  @Override
  public void processSyncDir(SyncDirId syncDirId, File syncDir) throws Exception {
    callbacks.processSyncDir(syncDirId, syncDir);
  }
}
