import java.nio.ByteBuffer;
import java.sql.*;

/*
Optimistic concurrency control requires a mechanism for checking the version of the data. To do this, it is necessary
to add a column in the database table that holds the version for the data row.

This column can be added by running the following statement on the database:

ALTER TABLE [Account] ADD [Version] [timestamp] NOT NULL

*/

public class OptimisticConcurrencyExercise {

	public void closeAccount(int closeAccountId, int transferAccountId) {

		String readAccountSql = "SELECT Balance, Version FROM Account WHERE Id = ?"; // Getting Version from database for comparison
		String closeAccountSql = "UPDATE Account SET Balance = 0 WHERE Id = ? AND Version = ?"; // Check on Version
		String depositSql = "UPDATE Account SET Balance = Balance + ? WHERE Id = ?"; // Check on Version is not necessary here, since it will not execute if the close account failed

		Connection conn = Database.getConnection(0);

		try {
			PreparedStatement readClosingAccount = conn.prepareStatement(readAccountSql);
			readClosingAccount.setInt(1, closeAccountId);
			ResultSet rs1 = readClosingAccount.executeQuery();
			if (rs1.next()) {
				float amount = rs1.getFloat(1);
				byte[] version = rs1.getBytes(2); // Reading version from closing account

				Program.printMsg("Transferring " + amount + " from accountId: " + closeAccountId + " Version: "
						+ ByteBuffer.wrap(version).getLong());

				PreparedStatement closeAccount = conn.prepareStatement(closeAccountSql);
				closeAccount.setInt(1, closeAccountId);
				closeAccount.setBytes(2, version); // Adding version to update statement
				int rows = closeAccount.executeUpdate();

				if (rows == 1) { // If the close account failed, we cannot transfer the money to account 2
					Program.printMsg("Account " + closeAccountId + " closed");

					PreparedStatement deposit = conn.prepareStatement(depositSql);
					deposit.setFloat(1, amount);
					deposit.setInt(2, transferAccountId);
					deposit.executeUpdate();

					Program.printMsg(amount + " Transferred to account " + transferAccountId);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void addInterest(int accountId) {

		float amount = 0;
		float interest = 0;
		byte[] version;

		String readAccountSql = "SELECT Balance, Version FROM Account WHERE Id = ?"; // Getting Version from database for comparison
		String addInterestSql = "UPDATE Account SET Balance = Balance + ? WHERE Id = ? AND Version = ?"; // Check on Version

		Connection conn = Database.getConnection(0);

		try {
			PreparedStatement readAccount = conn.prepareStatement(readAccountSql);

			readAccount.setInt(1, accountId);

			ResultSet rs = readAccount.executeQuery();
			if (rs.next()) {
				amount = rs.getFloat(1);
				version = rs.getBytes(2);

				Program.printMsg("Read amount: " + amount + " Version: " + ByteBuffer.wrap(version).getLong());

				interest = amount * 0.1f;

				PreparedStatement addInterest = conn.prepareStatement(addInterestSql);
				addInterest.setInt(2, accountId);
				addInterest.setBytes(3, version);
				addInterest.setFloat(1, interest);

				addInterest.execute();

				Program.printMsg("Added interest: " + interest);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
