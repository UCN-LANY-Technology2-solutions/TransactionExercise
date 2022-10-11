package optimisticConcurrencyExercise;

import java.sql.Connection;
import java.sql.SQLException;

import database.DataContext;

public class T2 extends AccountOperation implements Runnable {

	@Override
	public void run() {
		// TODO: Add interest of 10% to account 1

		try {
			Connection conn = DataContext.getConnection();

			Account account = this.readAccount(conn, 1);

			this.deposit(conn, 1, account.getBalance() * 0.1f);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
