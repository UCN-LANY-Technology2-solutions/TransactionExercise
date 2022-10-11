package database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

public class DataContext {

	public static Connection getConnection() throws SQLServerException {

		// TODO: Change the values below to connect to another instance of SQL Server

		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("student");
		ds.setPassword("P@$$w0rd");
		ds.setServerName("localhost\\sqlexpress");
		ds.setDatabaseName("TransactionExercises");
		return ds.getConnection();
	}

	public static Connection getConnection(int isolationLevel) throws SQLException {

		Connection conn = getConnection();
		conn.setTransactionIsolation(isolationLevel);
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
