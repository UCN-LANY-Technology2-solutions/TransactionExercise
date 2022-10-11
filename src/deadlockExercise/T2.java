package deadlockExercise;

import java.sql.Connection;
import java.sql.SQLException;

import database.DataContext;

public class T2 extends AccountOperation implements Runnable {

	@Override
	public void run() {
		// TODO: Read the balance of accounts of type B and create a new account of type A with that balance
		try {
			Connection conn = DataContext.getConnection(Connection.TRANSACTION_SERIALIZABLE);

			conn.setAutoCommit(false);

			try {

				float balanceB = this.readBalance(conn, "B");
				
				this.createAccount(conn, "a4", balanceB, "A");

				conn.commit();

			} catch (SQLException e) {

				conn.rollback();
				throw e;
			}
		} catch (Exception e) {

			e.printStackTrace();
		}
	}
}
