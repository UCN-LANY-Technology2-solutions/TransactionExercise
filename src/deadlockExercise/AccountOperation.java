package deadlockExercise;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AccountOperation {

	protected void createAccount(Connection conn, String name, float balance, String type) throws SQLException {

		String sql = "INSERT INTO Account (Name, Balance, Type) VALUES (?, ?, ?)";

		PreparedStatement createAccountStatement = conn.prepareStatement(sql);
		createAccountStatement.setString(1, name);
		createAccountStatement.setFloat(2, balance);
		createAccountStatement.setString(3, type);

		createAccountStatement.execute();

		System.out.println(Thread.currentThread().getName() + " Created account " + name);
	}

	protected float readBalance(Connection conn, String type) throws SQLException {

		String sql = "SELECT SUM(Balance) FROM Account WHERE Type = ?";

		PreparedStatement readBalance = conn.prepareStatement(sql);
		readBalance.setString(1, type);

		float result = 0;
		ResultSet record = readBalance.executeQuery();
		if (record.next()) {
			result = record.getFloat(1);
		}
		record.close();
		return result;
	}
}
