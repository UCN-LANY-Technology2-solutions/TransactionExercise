public class Program {

	public static void main(String[] args) {

		Thread t1 = null;
		Thread t2 = null;

		// Pessimistic Concurrency Exercise

		// Uncomment the line below to do the pessimistic concurrency exercise
//		PessimisticConcurrencyExercise ex = new PessimisticConcurrencyExercise();

		// Create instances of t1 and t2 that uses the PessimisticConcurrencyExercise
		// instance to
		// perform the following financial transactions

		// t1 - Transfer $100 from account 1 to account 2
//		t1 = new Thread("T1") {
//			public void run() {
//				ex.transferAmount(1,  2,  100);
//			}
//		};

		// t2 - Add interest of 10% to account 1
//		t2 = new Thread("T2") {
//			public void run() {
//				ex.addInterest(1, 0.1f);
//			}
//		};

		// Optimistic Concurrency Exercise

		// Uncomment the line below to do the optimistic concurrency exercise
//		OptimisticConcurrencyExercise ex = new OptimisticConcurrencyExercise();

		// Create instances of t1 and t2 that uses the OptimisticConcurrencyExercise
		// instance to
		// perform the following financial transactions

		// t1 - Close account 1 and deposit the amount from account 1 in account 2
//		t1 = new Thread("T1") {
//			public void run() {
//				ex.closeAccount(1, 2);
//			}
//		};
		// t2 - Add interest of 10% to account 1
//		t2 = new Thread("T2") {
//			public void run() {
//				ex.addInterest(1);
//			}
//		};

		// Deadlock Exercise

		// Uncomment the line below to do the deadlock exercise
		DeadlockExercise ex = new DeadlockExercise();

		// Create instances of t1 and t2 that uses the DeadlockExercise instance to
		// perform the following financial transactions

		// t1 - Transfer $100 from account 1 to account 2
		t1 = new Thread("T1") {
			public void run() {
				ex.transferAmount(1, 2, 100);
			}
		};
		// t2 - Transfer $50 from account 3 to account 2
		t2 = new Thread("T2") {
			public void run() {
				ex.transferAmount(3, 2, 50);
			}
		};
		
		t1.start();
		t2.start();
	}

	public static void printMsg(String msg) {
		System.out.println(Thread.currentThread().getName() + " - " + msg);
	}
}
