package demo;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.ibatis.migration.ConnectionProvider;
import org.apache.ibatis.migration.MigrationsLoader;
import org.apache.ibatis.migration.operations.UpOperation;
import org.apache.ibatis.migration.options.DatabaseOperationOption;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import demo.reflections.ReflectionsMigrationsLoader;
import demo.spring.PathMatchingResourceLoader;

public class CustomLoaderTest {

  private JdbcConnectionProvider connectionProvider;

  private DatabaseOperationOption dbOption;

  private ByteArrayOutputStream out;

  @Before
  public void setup() throws Exception {
    connectionProvider = new JdbcConnectionProvider("org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:javaapitest", "sa", "");
    dbOption = new DatabaseOperationOption();
    out = new ByteArrayOutputStream();
  }

  @After
  public void tearDown() throws Exception {
    runSql(connectionProvider, "shutdown");
  }

  @Test
  public void testPathMatchingLoaderOperation() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("changelog", "CHANGELOG");
    // Scripts is in /repo/migrations-demo-scripts-1.0.0.jar'.
    MigrationsLoader migrationsLoader = new PathMatchingResourceLoader("net/harawata/scripts", "utf-8", properties);

    new UpOperation().operate(connectionProvider, migrationsLoader, dbOption, new PrintStream(out));

    assertFalse(out.toString().contains("Error"));
    assertEquals("3", runQuery(connectionProvider, "select count(*) from changelog"));
    assertEquals("0", runQuery(connectionProvider, "select count(*) from first_table"));
    assertEquals("0", runQuery(connectionProvider, "select count(*) from second_table"));
  }

  @Test
  public void testReflectionsMigrationsLoaderOperation() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("changelog", "CHANGELOG");
    // Scripts is in /repo/migrations-demo-scripts-1.0.0.jar'.
    MigrationsLoader migrationsLoader = new ReflectionsMigrationsLoader("net.harawata.scripts", "utf-8", properties);

    new UpOperation().operate(connectionProvider, migrationsLoader, dbOption, new PrintStream(out));

    assertFalse(out.toString().contains("Error"));
    assertEquals("3", runQuery(connectionProvider, "select count(*) from changelog"));
    assertEquals("0", runQuery(connectionProvider, "select count(*) from first_table"));
    assertEquals("0", runQuery(connectionProvider, "select count(*) from second_table"));
  }

  protected void runSql(ConnectionProvider provider, String sql) throws SQLException {
    Connection connection = provider.getConnection();
    try {
      Statement statement = connection.createStatement();
      statement.execute(sql);
    } finally {
      connection.close();
    }
  }

  protected String runQuery(ConnectionProvider provider, String query) throws SQLException {
    Connection connection = provider.getConnection();
    try {
      Statement statement = connection.createStatement();
      ResultSet rs = statement.executeQuery(query);
      String result = null;
      if (rs.next()) {
        result = rs.getString(1);
      }
      return result;
    } finally {
      connection.close();
    }
  }

}
