import java.io.Serializable;

public class LogEntry implements Serializable{
	public int term;
	public int sum;
	public Record record;
	
	public LogEntry(int term , int sum , Record rec_) {     //,Record rec_){
		this.term 			= term ;
		this.sum		    = sum ;
		this.record         = rec_ ;
	}
	
	public void logRead(){
		System.out.println("\nLog information :");
		System.out.println("\nRecord sum =" + sum);
		System.out.println("\nLog Term =" + term);
		System.out.println("\nLog Record  : ");
		record.printValues();
	}
			
}
