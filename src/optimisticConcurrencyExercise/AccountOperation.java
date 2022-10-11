package optimisticConcurrencyExercise;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AccountOperation {

	protected Account readAccount(Connection conn, int id) throws SQLException {

		String sql = "SELECT Name, Balance, Version FROM Account WHERE Id = ?";

		PreparedStatement readBalance = conn.prepareStatement(sql);
		readBalance.setInt(1, id);
		ResultSet record = readBalance.executeQuery();

		Account result = null;

		while (record.next()) {
			result = new Account(id, record.getString(1), record.getBytes(3));
			result.setBalance(record.getFloat(2));

			System.out.println(result);
		}
		record.close();

		return result;
	}

	protected void closeAccount(Connection conn, int id) throws SQLException {

		String sql = "UPDATE Account SET Balance = 0 WHERE Id = ? ";
		PreparedStatement closeAccount = conn.prepareStatement(sql);
		closeAccount.setInt(1, id);
		closeAccount.executeUpdate();

		System.out.println("Account " + id + " closed");
	}

	protected void deposit(Connection conn, int id, float amount) throws SQLException {

		String sql = "UPDATE Account SET Balance = Balance + ? WHERE Id = ? ";
		PreparedStatement deposit = conn.prepareStatement(sql);
		deposit.setFloat(1, amount);
		deposit.setInt(2, id);
		deposit.executeUpdate();
	}
}
