package pessimisticConcurrencyExercise;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AccountOperation {

	protected void setBalance(Connection conn, int id, float amount) throws SQLException {

		String sql = "UPDATE Account SET Balance = ? WHERE Id = ?";

		PreparedStatement setBalanceStatement = conn.prepareStatement(sql);
		setBalanceStatement.setFloat(1, amount);
		setBalanceStatement.setInt(2, id);

		setBalanceStatement.execute();

		System.out.println(Thread.currentThread().getName() +" - Adjusted balance to " + amount);

	}

	protected float readBalance(Connection conn, int id) throws SQLException {

		String sql = "SELECT Balance FROM Account WHERE Id = ?";

		PreparedStatement readBalanceStatement = conn.prepareStatement(sql);
		readBalanceStatement.setInt(1, id);
		
		float result = 0;
		ResultSet record = readBalanceStatement.executeQuery();
		if (record.next()) {
			result = record.getFloat(1);
			System.out.println(Thread.currentThread().getName() +" - Account: " + id + " Balance: " + result);
		}
		record.close();
		return result;
	}
}
