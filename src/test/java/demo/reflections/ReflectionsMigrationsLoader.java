package demo.reflections;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.ibatis.migration.Change;
import org.apache.ibatis.migration.MigrationException;
import org.apache.ibatis.migration.MigrationReader;
import org.apache.ibatis.migration.MigrationsLoader;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

/**
 * Sample implementation of MigrationsLoader which internally uses Reflections (https://code.google.com/p/reflections/).
 */
public class ReflectionsMigrationsLoader implements MigrationsLoader {

  private String scriptsPackage;

  private String charset;

  private Properties properties;

  public ReflectionsMigrationsLoader(String scriptsPackage, String charset, Properties properties) {
    super();
    this.scriptsPackage = scriptsPackage;
    this.charset = charset;
    this.properties = properties;
  }

  @Override
  public List<Change> getMigrations() {
    List<Change> changes = new ArrayList<Change>();
    Set<String> filenames = findScripts(".*_.*\\.sql");
    for (String filename : filenames) {
      int lastSlashPos = filename.lastIndexOf('/');
      Change change = parseChangeFromFilename(filename.substring(lastSlashPos + 1));
      changes.add(change);
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
      Set<String> filenames = findScripts(change.getFilename().replaceAll("\\.", "\\."));
      Reader reader = getScriptAsReader(filenames, undo);
      if (reader == null) {
        throw new MigrationException("Error finding script " + change.getFilename());
      }
      return reader;
    } catch (IOException e) {
      throw new MigrationException("Error reading " + change.getFilename(), e);
    }
  }

  protected Reader getScriptAsReader(Set<String> filenames, boolean undo) throws IOException {
    for (Iterator<String> iterator = filenames.iterator(); iterator.hasNext();) {
      String filename = (String) iterator.next();
      return new MigrationReader(getClass().getClassLoader().getResourceAsStream(filename), charset, undo, properties);
    }
    return null;
  }

  @Override
  public Reader getBootstrapReader() {
    try {
      Set<String> filenames = findScripts("bootstrap\\.sql");
      Reader reader = getScriptAsReader(filenames, false);
      if (reader == null) {
        throw new MigrationException("Error finding bootstrap.sql.");
      }
      return reader;
    } catch (IOException e) {
      throw new MigrationException("Error reading bootstrap.sql", e);
    }
  }

  protected Set<String> findScripts(String searchPattern) {
    return new Reflections(scriptsPackage, new ResourcesScanner()).getResources(Pattern.compile(searchPattern));
  }
}
