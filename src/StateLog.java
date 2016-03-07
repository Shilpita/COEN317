import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Vector;

public class StateLog implements StateLogInterface{
	public 	Vector<LogEntry>stateLog   ;
	
	public static void main(String[] args) {
		try {
            // RMI interface
			   StateLog class_obj = new StateLog();
			   Registry registry = LocateRegistry.getRegistry(8080);
			   StateLogInterface serverStateLog = (StateLogInterface) UnicastRemoteObject.exportObject(class_obj, 8080);
	           registry.rebind("StateLog", serverStateLog);
			   System.out.println("State Log Server is ready.");
			   
		   }catch (Exception e) {
			   System.out.println("State Log Server failed: " + e.toString());
			}	 

	}

	@Override
	public int commitToStateLog(LogEntry newLogEntry) throws RemoteException {
		stateLog.add(newLogEntry);
		return stateLog.size();
	}

}
