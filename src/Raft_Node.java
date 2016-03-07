

import java.rmi.*;
import java.util.Iterator;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.*;

import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;



public class Raft_Node  implements RaftRequestInterface {
	
    public static Registry registry; 
    public static RaftRequestInterface server_if;
	public static final int MaxNodes = 5 ;
	private static final int min = 2;
	private static final int max = 20;
    private static final int delay = 5000;
    
    private int sum;
    public Record writeRecord ;
	
	public int currentTerm ;
	public int prevLogEntryTerm ;
	public int     votedCurrTerm;
	private int leaderTerm;
	
	public static int prevLogEntryIndex;
	public static int newLogEntryIndex;
	
	public static LogEntry prevCommitLogEntry  = null;
	public static LogEntry newRecvLogEntry = null;
	public LogEntry currentToComitLogEntry = null;
	public LogEntry prevCandidateLogEntry = null;
	
	public static	Vector<LogEntry>stateLog = null;
	public 	Vector<LogEntry>localLog  = null ;
	
	public static int appendEntryCount ;
	public static int recvVoteCount ;
	public RaftTimer timer_ ;
	
	 // Range for the period to check if heard from leader, [1,5]

	private int silencePeriod; 
	private int messagePeriod;
	
    
	private String host;  
	private String name;
	
	public static String leaderId;
   
	public static int hb = 0;
    
	public boolean heardFromLeader = false;
	public boolean noLeaderFound   = true;

    
	enum State {
					LEADER,
					FOLLOWER,
					CANDIDATE,
					NONE
				}   
	public State state_;
	

/*  
    public static void rebindForLeader(RaftRequestInterface server_if) throws RemoteException, AlreadyBoundException {
            registry.bind("ArithmeticModule",server_if);
            System.out.println("Arithmetic bounded for leader"+leaderId);
        }
        
 */       
        
        
	  
    public LogEntry createLogEntry() {

            int x = (min + (int)(Math.random() * ((max - min) + 1))) * 1000;
            int y = (min + (int)(Math.random() * ((max - min) + 1))) * 1000;
            System.out.println("\n X ="+ x);
            System.out.println("\n Y ="+ y);
            writeRecord = new Record(x ,y);
             sum = x+y;
            System.out.println("The sum is"+sum);   
           
            LogEntry newLogEntry = new LogEntry(leaderTerm , sum ,writeRecord);
            localLog.add(newRecvLogEntry);

            //for(int i =0; i< localLog.size() ;i++ ){
            	 //LogEntry log = localLog.get(i);
            	 //log.logRead();
           // }
   
            newLogEntryIndex = localLog.size();
            System.out.println("The newLogEntryIndex is"+newLogEntryIndex);
            return newLogEntry;
    }
    

	
	@SuppressWarnings("unused")
	private Raft_Node() throws RemoteException {super();}
	
	
	public int ComputeElectionTimeout(){
		    silencePeriod =  (min + (int)(Math.random() * ((max - min) + 1))) * 1000;
		    return silencePeriod;
	}

	public Raft_Node(String nameIn, String hostIn) throws RemoteException {
		//super();
		
		this.name 			= nameIn;
		this.host 			= hostIn;
		this.state_ 		= State.FOLLOWER;
		this.localLog 		= new Vector<LogEntry>();
		this.votedCurrTerm  = 0;
		this.currentTerm    = 0;
		this.currentToComitLogEntry = null ;
		System.out.println("\n Inside constructor.." + this.name );
		this.silencePeriod  = ComputeElectionTimeout();
		
		// Make sure that messages are getting sent more frequently then
		// the node checks for silence
		
	
		messagePeriod = 4000;
		System.out.println("\n "+name + "initiated.message sending interval " + messagePeriod);
		System.out.println("\n "+name + "initiated.Silence  interval " + silencePeriod);
		timer_ = new RaftTimer(); 
			
	}

	class RaftTimer extends Timer {

		    private RaftElectionTask electionTask_;
		    private RaftHearbeatTask appendEntryTask_;
		    
			public RaftTimer() {
				//electionTask_    = new RaftElectionTask();
				appendEntryTask_ = new RaftHearbeatTask();
				System.out.println("\n Scheduling election task.");
				//schedule(electionTask_, 0, silencePeriod); 
				System.out.println("\n Scheduling message sending.");
				schedule(appendEntryTask_, delay, messagePeriod); 
			}
	}
	
	class RaftElectionTask extends TimerTask {
			public void run(){
				try {
					Registry reg = LocateRegistry.getRegistry();
					System.out.println("\n Inside Election task.....");
					votedCurrTerm =0;
					recvVoteCount =0;
				   // if (!state_.equals(State.LEADER) && !heardFromLeader ){    //&& votedCurrTerm == 0) {
					if (state_.equals(State.CANDIDATE)) { 
								//state_      = State.CANDIDATE ;
								currentTerm ++;
								votedCurrTerm ++;     // vote Flag
								recvVoteCount ++;     // ballot
								int prevCandidateLogEntryIndex  = localLog.size();
								System.out.println("\n Candidate log size :" + prevCandidateLogEntryIndex);
								if(prevCandidateLogEntryIndex > 0){
								   prevCandidateLogEntry = localLog.lastElement();
								   
								}
								System.out.println("\n Start Election with Candidate node : "+ name + "\t The election term is :"+ currentTerm);
						        System.out.println("\n Starting Election");
								for (String nodeName : reg.list()) {
									System.out.println("\n nodeName :"+ nodeName);
										RaftRequestInterface otherNodeStub = (RaftRequestInterface) reg.lookup(nodeName);
										int voteReply = otherNodeStub.voteRequest(currentTerm , name , prevCandidateLogEntry , prevCandidateLogEntryIndex );
										if (voteReply > 0){
											recvVoteCount ++ ;
										}
									}
								System.out.println("\n Candidate node : "+ name + "\t The election term is :"+ currentTerm +"\t The received vote this term :"+ recvVoteCount);
								if(recvVoteCount >=  (MaxNodes/2) +1) {
										state_    = State.LEADER;	
										leaderId  = name ;
										leaderTerm = currentTerm;
									    prevLogEntryIndex = prevCandidateLogEntryIndex ;
									    LogEntry prevLogEntry = prevCandidateLogEntry;
									    
									  //  createLogEntry();                 
                                                                                
										for (String nodeName1 : reg.list()) {
											    RaftRequestInterface otherNodeStub = (RaftRequestInterface) reg.lookup(nodeName1);
												int appendEntryRequest = otherNodeStub.appendEntryRequest(null,leaderTerm, leaderId, prevLogEntry ,prevLogEntryIndex);
												System.out.println("declared new leader \t "+name + " is the new leader for term "+ currentTerm);
											}
										
										timer_.electionTask_.cancel(); // election over
									}
								votedCurrTerm = 0;
						}
 			
				    }catch (RemoteException | NotBoundException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
				}
			}
}
	
	class RaftHearbeatTask extends TimerTask {
		public void run(){
			
			try {
			     	Registry reg = LocateRegistry.getRegistry(host);
			
			if (state_ == State.LEADER){
				if (leaderTerm < currentTerm) { // revisit
					state_ = State.FOLLOWER;
					
				} else { 
							final int TEST001 = 200;
				     
							if (hb < TEST001) {    
								                   if((stateLog != null) && stateLog.isEmpty()){
								                	     prevCommitLogEntry = null;
							                        	 prevLogEntryIndex  = 0;
							                        	 prevLogEntryTerm   = 0;
							                         }else if (stateLog == null) {
							                        	 stateLog = new Vector<LogEntry>();
							                         } else {
							                        	 prevCommitLogEntry  = stateLog.lastElement();
												         prevLogEntryIndex   = stateLog.size();
												         prevCommitLogEntry.logRead();
												         prevLogEntryTerm    = prevCommitLogEntry.term;
							                         }
							
												     newRecvLogEntry     = createLogEntry();
												     
													  if(appendEntryCount >= (MaxNodes/2) +1){
														    stateLog.addElement(newRecvLogEntry);
														    appendEntryCount =0;
													  }else{
													          for (String nodeName1 : reg.list()) {
														                 System.out.println("\n nodeName1 :"+ nodeName1);
																		 RaftRequestInterface otherNodeStub = (RaftRequestInterface) reg.lookup(nodeName1);
																		 int appendEntryRequest = otherNodeStub.appendEntryRequest(newRecvLogEntry,leaderTerm, leaderId, prevCommitLogEntry ,prevLogEntryIndex);
																		 System.out.println(name + " is the new leader for term "+ currentTerm);
																		 if(appendEntryRequest == 1){
																			           appendEntryCount++;
																		 }
													          	}
													  }
													 hb++; 
							 					} else {
								                            hb = 0;
							                    } 
				      }
			} else if (state_ == State.CANDIDATE) { 
				if (heardFromLeader) {
					timer_.electionTask_.cancel();
					heardFromLeader = false;
				    state_ = State.FOLLOWER;
				    System.out.println(name + " stepped down.");
				   
				} 
				 
			} else if (state_ == State.FOLLOWER) {   
				if (heardFromLeader) {
					heardFromLeader = false;
				} else {
					state_ = State.CANDIDATE;
					System.out.println(name + " No Hearbeat from leader "+ currentTerm);
					timer_.electionTask_    = new RaftElectionTask();
					timer_.schedule(timer_.electionTask_, 0, silencePeriod);
				}
			}
						
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
	}
	
	
	public int appendEntryRequest(LogEntry newLogEntry, int leaderTerm, String leaderName, LogEntry lastCommitLogEntry,
			int lastCommitedLogIndex) throws RemoteException {
		
		if (newLogEntry == null){
			System.out.println(" leader id "+ leaderName);
		     heardFromLeader = true;
		     return 0;
		}else{
			   
			   System.out.println(" leader id "+ leaderName);
			   System.out.println("\n Node name :" + name + " for current term :"+ currentTerm);
               if(localLog != null && !(localLog.isEmpty())){
			     LogEntry lastCommitedLogEntry = localLog.lastElement();
			    int currentToCommmitLogIndex = localLog.size() +1 ;
			    if(currentTerm <= leaderTerm && currentTerm <= newLogEntry.term){
				           if(lastCommitLogEntry == null && lastCommitedLogIndex == 0){
				        	   currentToComitLogEntry = newLogEntry;
				           }else if(lastCommitLogEntry.term <= currentToComitLogEntry.term && currentToCommmitLogIndex <= lastCommitedLogIndex){
							    localLog.add(currentToComitLogEntry);
							    currentToComitLogEntry = newLogEntry;
							    System.out.println(" Node " + name + "comitted following log entry to Local state machine :");
							    localLog.lastElement().logRead();
						   }
			   }else{
				   return 0;
			   } 
              }else{
            	  currentToComitLogEntry = newLogEntry;
               }
               
			   heardFromLeader = true;
			   return 1;
		}
		
	}
      

	@Override
	public int voteRequest(int newTerm, String candidateName, LogEntry lastCandidateCommitedEntry,
			int lastCandidateCommitedIndex) throws RemoteException {
		int currentLogIndex  = localLog.size();
         if (newTerm < currentTerm ||  (newTerm == currentTerm && currentLogIndex > lastCandidateCommitedIndex) || votedCurrTerm != 0) {
					System.out.println("\n Vote denied by " + name + "\t to Candidate "+ candidateName +"\t for term "+ newTerm);
			        return 0;
		 }else{  
			      currentTerm    = newTerm;
			      votedCurrTerm ++;
			      System.out.println("\n Vote given by " + name + "\t to Candidate "+ candidateName +"\t for term "+ newTerm);
		          return 1;
		 }
	}
	
	public static void main(String[] args) {
		
		        String name = "Node-" + System.currentTimeMillis();
		        
			    String hostname = (args.length < 2) ? null : args[1];
				try {
					   Raft_Node class_obj = new Raft_Node(name,hostname);
					   registry = LocateRegistry.getRegistry();
					   server_if = (RaftRequestInterface) UnicastRemoteObject.exportObject(class_obj, 0);
                      
					   registry.bind(name, server_if);
				   //  System.out.println("RMI_Hello Server is ready." + name);
				
				} catch (Exception e) {
					System.out.println("Node Error: " + e.toString());
					e.printStackTrace();
				}
	}
	
}
