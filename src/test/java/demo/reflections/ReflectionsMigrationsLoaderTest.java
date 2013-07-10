package demo.reflections;

import static org.junit.Assert.*;

import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Properties;

import org.apache.ibatis.migration.Change;
import org.junit.Test;

public class ReflectionsMigrationsLoaderTest {

  @Test
  public void testGetMigrations() throws Exception {
    ReflectionsMigrationsLoader loader = createMigrationsLoader();
    List<Change> migrations = loader.getMigrations();
    assertEquals(3, migrations.size());
  }

  @Test
  public void testGetScriptReader() throws Exception {
    ReflectionsMigrationsLoader loader = createMigrationsLoader();
    Change change = new Change();
    change.setFilename("20130707120738_create_first_table.sql");
    Reader reader = loader.getScriptReader(change, false);
    Writer writer = new StringWriter();
    int c;
    while ((c = reader.read()) != -1) {
      writer.write(c);
    }
    assertTrue(writer.toString().indexOf("CREATE TABLE first_table (\nID INTEGER NOT NULL,\nNAME VARCHAR(16)\n);") > -1);
  }

  @Test
  public void testGetBootstrapReader() throws Exception {
    ReflectionsMigrationsLoader loader = createMigrationsLoader();
    Reader reader = loader.getBootstrapReader();
    Writer writer = new StringWriter();
    int c;
    while ((c = reader.read()) != -1) {
      writer.write(c);
    }
    assertTrue(writer.toString().indexOf("CREATE TABLE bootstrap_table (\nID INTEGER NOT NULL,\nNAME VARCHAR(16)\n);") > -1);
  }

  protected ReflectionsMigrationsLoader createMigrationsLoader() {
    Properties properties = new Properties();
    properties.setProperty("changelog", "CHANGELOG");
    ReflectionsMigrationsLoader loader = new ReflectionsMigrationsLoader("net.harawata.scripts", "utf-8", properties);
    return loader;
  }
}
