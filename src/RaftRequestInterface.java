

import java.rmi.Remote;
import java.rmi.RemoteException;

//Raft RMI request

public interface RaftRequestInterface extends Remote {
   
	public int appendEntryRequest(LogEntry newLogEntry, int newLogIndex , String leaderName, LogEntry lastCommitedEntry, int lastCommitedLogIndex) throws RemoteException ;
	
//	public int appendEntryReply() throws RemoteException ;
	
	public int voteRequest(int newTerm , String candidateName , LogEntry lastCandidateCommitedEntry , int lastCandidateCommitedIndex ) throws RemoteException ;

	
	
	
	
}
