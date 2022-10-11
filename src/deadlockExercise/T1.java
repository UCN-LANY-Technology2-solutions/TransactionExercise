package deadlockExercise;

import java.sql.Connection;
import java.sql.SQLException;

import database.DataContext;

public class T1 extends AccountOperation implements Runnable {

	@Override
	public void run() {
		// TODO:  Read the balance of accounts of type A and create a new account of type B with that balance
		
		try {
			Connection conn = DataContext.getConnection(Connection.TRANSACTION_SERIALIZABLE);

			conn.setAutoCommit(false);

			try {

				float balanceA = this.readBalance(conn, "A");
				
				this.createAccount(conn, "b1", balanceA, "B");

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
