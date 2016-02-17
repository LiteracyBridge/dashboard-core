package org.literacybridge.dashboard.config;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.File;

/**
 * Loads the appropriate deployment dependent properties from property files for configuration.  This class will look for
 * properties in 3 different places:
 * <ol>
 * <li>The spring/default.properties file, located in the resources/spring directory.  This is where defaults are stored.</li>
 * <li>The ./dashboard.properties file on the file system.  This is mainly for developers that are testing things locally</li>
 * <li>The /opt/literacybridge/dashboard.properties file on the file system.  This is mainly for production deploys</li>
 * </ol>
 */
@Configuration
public class PropertiesConfig {

  public static final String FsAgnostify(String fsPath) {
    return fsPath.replace('/', File.separatorChar);
  }

  @Bean
  public PropertyPlaceholderConfigurer propertyPlaceholderConfigurer() {
    PropertyPlaceholderConfigurer propertyPlaceholderConfigurer = new PropertyPlaceholderConfigurer();
    propertyPlaceholderConfigurer.setIgnoreResourceNotFound(true);
    propertyPlaceholderConfigurer.setLocations(new Resource[]{
        new ClassPathResource("spring/default.properties"),
        new FileSystemResource(FsAgnostify("/opt/literacybridge/dashboard.properties")),
        new FileSystemResource(FsAgnostify("./dashboard.properties"))
    });

    // Allow for other PropertyPlaceholderConfigurer instances.
    propertyPlaceholderConfigurer.setIgnoreUnresolvablePlaceholders(true);
    return propertyPlaceholderConfigurer;
  }
}
