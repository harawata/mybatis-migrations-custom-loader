package demo.spring;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.ibatis.migration.Change;
import org.apache.ibatis.migration.MigrationException;
import org.apache.ibatis.migration.MigrationReader;
import org.apache.ibatis.migration.MigrationsLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Sample implementation of MigrationsLoader which internally uses Spring's PathMatchingResourcePatternResolver.
 */
public class PathMatchingResourceLoader implements MigrationsLoader {

  private PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

  private String scriptsDir;

  private String charset;

  private Properties properties;

  public PathMatchingResourceLoader(String scriptsDir, String charset, Properties properties) {
    super();
    this.scriptsDir = scriptsDir;
    this.charset = charset;
    this.properties = properties;
  }

  @Override
  public List<Change> getMigrations() {
    List<Change> changes = new ArrayList<Change>();
    try {
      Resource[] resources = resolver.getResources(scriptsDir + "/*_*.sql");
      for (Resource resource : resources) {
        Change change = parseChangeFromFilename(resource.getFilename());
        changes.add(change);
      }
    } catch (IOException e) {
      throw new MigrationException("Error retrieving resources.  Cause: " + e, e);
    }
    return changes;
  }

  private Change parseChangeFromFilename(String filename) {
    try {
      Change change = new Change();
      String[] parts = filename.split("\\.")[0].split("_");
      change.setId(new BigDecimal(parts[0]));
      StringBuilder builder = new StringBuilder();
      for (int i = 1; i < parts.length; i++) {
        if (i > 1) {
          builder.append(" ");
        }
        builder.append(parts[i]);
      }
      change.setDescription(builder.toString());
      change.setFilename(filename);
      return change;
    } catch (Exception e) {
      throw new MigrationException("Error parsing change from file.  Cause: " + e, e);
    }
  }

  @Override
  public Reader getScriptReader(Change change, boolean undo) {
    try {
      Resource resource = resolver.getResource(scriptsDir + "/" + change.getFilename());
      if (!resource.exists()) {
        throw new MigrationException("Error finding script " + change.getFilename());
      }
      return new MigrationReader(resource.getFile(), charset, undo, properties);
    } catch (IOException e) {
      throw new MigrationException("Error reading " + change.getFilename(), e);
    }
  }

  @Override
  public Reader getBootstrapReader() {
    try {
      Resource resource = resolver.getResource(scriptsDir + "/bootstrap.sql");
      if (!resource.exists()) {
        throw new MigrationException("Error finding bootstrap.sql.");
      }
      return new MigrationReader(resource.getFile(), charset, false, properties);
    } catch (IOException e) {
      throw new MigrationException("Error reading bootstrap.sql", e);
    }
  }
}
