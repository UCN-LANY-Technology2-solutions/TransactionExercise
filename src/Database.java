import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.osgi.SQLServerDataSourceFactory;

public class Database {

	public static Connection getConnection(int isolationLevel) {

		Connection conn = null;

		try {
			// Change the values below to connect to another instance of SQL Server
			SQLServerDataSource ds = new SQLServerDataSource();
			ds.setUser("student");
			ds.setPassword("P@$$w0rd");
			ds.setServerName("localhost\\sqlexpress");
			ds.setDatabaseName("TransactionExercises");
			
			conn = ds.getConnection();
			conn.setTransactionIsolation(isolationLevel);			

		} catch (SQLException e) {

			e.printStackTrace();
		}

		return conn;
	}

	public static void printSessionInfo(Connection conn) {

		PreparedStatement selectSessionInfo = null;

		try {
			selectSessionInfo = conn.prepareStatement("select @@SPID");
			ResultSet sessionInfoRows = selectSessionInfo.executeQuery();
			sessionInfoRows.next();
			System.out.println(Thread.currentThread().getName() + " - Session: " + sessionInfoRows.getInt(1)
					+ ", IsolationLevel: " + conn.getTransactionIsolation());

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
