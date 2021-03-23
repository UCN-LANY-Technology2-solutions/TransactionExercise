import java.sql.*;

public class PessimisticConcurrencyExercise {
	
//  The correct isolationlevel is repeatable_read, but that will actually introduce a risk of a deadlock. 

	private final int isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;

	public void transferAmount(int fromAccountId, int toAccountId, float amount) {

		String withdrawSql = "UPDATE Account SET Balance = Balance - ? WHERE Id = ?";
		String depositSql = "UPDATE Account SET Balance = Balance + ? WHERE Id = ?";
		String readBalanceSql = "SELECT Name, Balance FROM Account WHERE Id = ? OR Id = ?";

		Connection conn = Database.getConnection(isolationLevel);
		try {
			try {

				conn.setAutoCommit(false);

				PreparedStatement withdraw = conn.prepareStatement(withdrawSql);
				withdraw.setFloat(1, amount);
				withdraw.setInt(2, fromAccountId);

				withdraw.execute();

				Program.printMsg("Withdrew: " + amount);

				PreparedStatement deposit = conn.prepareStatement(depositSql);
				deposit.setFloat(1, amount);
				deposit.setFloat(2, toAccountId);

				deposit.execute();

				Program.printMsg("Deposited: " + amount);

				PreparedStatement readBalance = conn.prepareStatement(readBalanceSql);
				readBalance.setInt(1, fromAccountId);
				readBalance.setInt(2, toAccountId);

				ResultSet rs = readBalance.executeQuery();
				while (rs.next()) {

					Program.printMsg("Account: " + rs.getString(1) + " Balance: " + rs.getFloat(2));
				}

				conn.commit();
			}
			catch (SQLException e) {

				conn.rollback();
				throw e;

			} finally {

				conn.setAutoCommit(true);
			}
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public void addInterest(int accountId, float interest) {

		float amount = 0;
		float interestAmount = 0;

		String readBalanceSql = "SELECT Balance FROM Account WHERE Id = ?";
		String addInterestSql = "UPDATE Account SET Balance = ? WHERE Id = ?";

		Connection conn = Database.getConnection(isolationLevel);

		try {
			try {

				conn.setAutoCommit(false);

				PreparedStatement readBalance = conn.prepareStatement(readBalanceSql);
				readBalance.setInt(1, accountId);

				ResultSet rs = readBalance.executeQuery();
				if (rs.next()) {
					amount = rs.getFloat(1);

					Program.printMsg("Read amount: " + amount);

					interestAmount = amount * interest;

					PreparedStatement addInterest = conn.prepareStatement(addInterestSql);
					addInterest.setInt(2, accountId);
					addInterest.setFloat(1, amount + interestAmount);

					addInterest.execute();  // aquires an exclusive lock on account, so if that is not released by the other transaction, a deadlock will occur

					Program.printMsg("Added interest: " + interestAmount);
				}

				conn.commit();

			} catch (SQLException e) {

				conn.rollback();
				throw e;

			} finally {

				conn.setAutoCommit(true);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}