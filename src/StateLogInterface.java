
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface StateLogInterface extends Remote {
	
	public int commitToStateLog (LogEntry newLogEntry)throws RemoteException ;

}
