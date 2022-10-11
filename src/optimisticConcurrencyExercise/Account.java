package optimisticConcurrencyExercise;

public class Account {
	
	private int id;
	private String name;
	private float balance;
	private byte[] version;
	
	public int getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public float getBalance() {
		return balance;
	}

	public void setBalance(float balance) {
		this.balance = balance;
	}

	public byte[] getVersion() {
		return version;
	}

	public Account(int id, String name, byte[] version) {
		super();
		this.id = id;
		this.name = name;
		this.version = version;
	}
	
	@Override
	public String toString() {
		return Thread.currentThread().getName() + " - Account "+ id +" ("+ balance +")";
	}
	
}
