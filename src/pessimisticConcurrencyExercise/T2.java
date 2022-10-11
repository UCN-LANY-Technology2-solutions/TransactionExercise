package pessimisticConcurrencyExercise;

import java.sql.Connection;
import java.sql.SQLException;

import database.DataContext;

public class T2 extends AccountOperation implements Runnable {

	@Override
	public void run() {
		// TODO: Transfer $100 from account 1 to account 3

		try {
			Connection conn = DataContext.getConnection(Connection.TRANSACTION_READ_COMMITTED);
			conn.setAutoCommit(false);

			try {

				float balance1 = this.readBalance(conn, 1);
				float balance3 = this.readBalance(conn, 3);
				
				this.setBalance(conn, 1, balance1 - 100);
				this.setBalance(conn, 3, balance3 + 100);

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
