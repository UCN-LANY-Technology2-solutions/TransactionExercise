import java.sql.*;

public class DeadlockExercise {
	
//	This is a very special case of deadlock. It only implicates a single table, however, the isolationlevel of 
//	READ_COMMITTED places a lock on the row that will be released on commit. So the two transactions here wants 
//	to place a lock on the row with id = 2, but since both transactions waits for the other to commit, the deadlock 
//  happens.
	
//	This is a scenario with no safe solution. Even using a more restrictive isolation level like SERIALIZABLE will 
//  not always work because it only locks records within the selected data set, in this case it is still possible
//  for one transaction to place a lock on a record, that the other will try to read. 

//  This is due to the phenomenon where the database system is allowed to physically overlap the execution of 
//  serializable transactions in time (thereby increasing concurrency) so long as the effects of those transactions 
//  still correspond to some possible order of serial execution. In other words, serializable transactions are only 
//  potentially serializable and not actually serialized.	

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
				withdraw.close();
				
				Program.printMsg("Withdrew: "+ amount);
				
				PreparedStatement deposit = conn.prepareStatement(depositSql);
				deposit.setFloat(1, amount);
				deposit.setFloat(2, toAccountId);
				
				deposit.execute();
				deposit.close();
				
				Program.printMsg("Deposited: "+ amount);
				
				PreparedStatement readBalance = conn.prepareStatement(readBalanceSql);		
				readBalance.setInt(1, fromAccountId);
				readBalance.setInt(2, toAccountId);
				
				ResultSet rs = readBalance.executeQuery();
				while(rs.next()) {
					
					Program.printMsg("Account: "+ rs.getString(1) +" Balance: "+ rs.getFloat(2));
				}
				
				readBalance.close();
				conn.commit();		
				
			} catch (SQLException e) {

				conn.rollback();
				System.out.println("Transaction rolled back");

				e.printStackTrace();
			} 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
