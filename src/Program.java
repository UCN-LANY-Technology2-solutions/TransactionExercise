public class Program {

	public static void main(String[] args) {

		// TODO: Pessimistic Concurrency Exercise 
		// Uncomment the two lines below to work with the exercise
//		Thread t1 = new Thread(new pessimisticConcurrencyExercise.T1(), "T1");
//		Thread t2 = new Thread(new pessimisticConcurrencyExercise.T2(), "T2");
		
		// TODO: Optimistic Concurrency Exercise
		// Uncomment the two lines below to work with the exercise
		Thread t1 = new Thread(new optimisticConcurrencyExercise.T1(), "T1");
		Thread t2 = new Thread(new optimisticConcurrencyExercise.T2(), "T2");
	
		// TODO: Deadlock Exercise
		// Uncomment the two lines below to work with the exercise
//		Thread t1 = new Thread(new deadlockExercise.T1(), "T1");
//		Thread t2 = new Thread(new deadlockExercise.T2(), "T2");
		
		t1.start();
		t2.start();
	}

	public static void printMsg(String msg) {
		System.out.println(Thread.currentThread().getName() + " - " + msg);
	}
}
