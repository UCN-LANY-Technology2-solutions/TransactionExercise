import java.sql.*;

public class DeadlockExercise {
	
//	This is a very special case of deadlock. It only implicates a single table, however, the isolationlevel of READ_COMMITTED
//	places a lock on the row that will be released on commit. So the two transactions here wants to place a lock on the row 
//	with id = 2, but since both transactions waits for the other to commit, the deadlock happens.
	
//	This is a scenarios where the only solution is to actually use a more restrictive isolation level. SERIALIZABLE places a
//	lock on the entire table, so the last transaction cannot start before the other is finished. That will solve the issue, 
//	but also lower the performance of the system

	private final int isolationLevel = Connection.TRANSACTION_SERIALIZABLE;

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
				
				Program.printMsg("Withdrew: "+ amount);
				
				PreparedStatement deposit = conn.prepareStatement(depositSql);
				deposit.setFloat(1, amount);
				deposit.setFloat(2, toAccountId);
				
				deposit.execute();
				
				Program.printMsg("Deposited: "+ amount);
				
				PreparedStatement readBalance = conn.prepareStatement(readBalanceSql);		
				readBalance.setInt(1, fromAccountId);
				readBalance.setInt(2, toAccountId);
				
				ResultSet rs = readBalance.executeQuery();
				while(rs.next()) {
					
					Program.printMsg("Account: "+ rs.getString(1) +" Balance: "+ rs.getFloat(2));
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
