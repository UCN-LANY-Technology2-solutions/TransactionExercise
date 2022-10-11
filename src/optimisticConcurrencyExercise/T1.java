package optimisticConcurrencyExercise;

import java.sql.Connection;

import database.DataContext;

public class T1 extends AccountOperation implements Runnable {

	@Override
	public void run() {
		// TODO: Close account 1 and deposit the amount from account 1 in account 2

		try {
			Connection conn = DataContext.getConnection();

			Account account = this.readAccount(conn, 1);

			this.closeAccount(conn, 1);
			
			this.deposit(conn, 2, account.getBalance());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

}
