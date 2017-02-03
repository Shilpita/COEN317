/******************************************************************************
 * COEN 317 - Distributed Systems Project -Winter2016
 * author: Shilpita Roy (sroy) , Sruthi
 * Project: Raft algorithm
 ***************************************************************************/

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



public class Node  implements RaftRequestInterface {
	
	public static final int MaxNodes = 5 ;
	private static final int min = 2;
	private static final int max = 20;
    private static final int delay = 5000;
    final int TEST001 = 20;
    
   	public RaftTimer timer_ ;
   	private int silencePeriod; 
   	private int messagePeriod;
    
	public int currentTerm ;
	public int prevLogEntryTerm ;
	public int votedCurrTerm;
	private int leaderTerm;
	public boolean heardFromLeader = false;
	public boolean noLeaderFound   = true;
	public int recvVoteCount;
	
	public  Repl_TwoPhaseCommit repl_;
	public 	Vector<LogEntry>localLog;
	public static Vector<LogEntry>stateLog = null;
    private int sum;
    public Record writeRecord ;
	
	public int appendEntryCount;

	private String host;  
	private String name;
	public String leaderId;
   
	public static int hb = 0;
    
	enum State {
					LEADER,
					FOLLOWER,
					CANDIDATE,
					NONE
				}   
	
   public State state_;

   public int debug_state = 1;
   
   public void StateTrace(State trace_id, String msg) {
	   
	   if (0 == debug_state)
		   return;
	   
	   switch (trace_id) {
	   
	   case LEADER :
		   System.out.println("LEADER_DBG: " + this.name + " : " + msg);
		   break;
	   case FOLLOWER :
		   System.out.println("FOLLOWER_DBG: " + this.name + " : " + msg);
		   break; 
	   case CANDIDATE :
		   System.out.println("CANDIDATE_DBG: " + this.name + " : " + msg);
		   break;   
	   default:
		   break;   	   
	   }
	   
   }
   
 public int debug_repl = 1;
 
 public void ReplTrace(State trace_id, String msg) {
	   
	   if (0 == debug_repl)
		   return;
	   
	   switch (trace_id) {
	   
	   case LEADER :
		   System.out.println("LEADER_DBG_REP: " + this.name + " : " + msg);
		   break;
	   case FOLLOWER :
		   System.out.println("FOLLOWER_DBG_REP: " + this.name + " : " +  msg);
		   break; 
	   case CANDIDATE :
		   System.out.println("CANDIDATE_DBG_REP: " + this.name + " : " + msg);
		   break;   
	   default:
		   break;   	   
	   }
	   
   }
 
    public class Repl_TwoPhaseCommit {
 	
 	    public int local_last_index_;  // used by non-leader  
 	    public int state_last_index_;  // used by leader
 	    public LogEntry flush_entry_;
 	    public LogEntry last_entry_;
 	
 	    public Repl_TwoPhaseCommit() {
 		   local_last_index_ = -1;
 		   state_last_index_ = -1;
 		   flush_entry_ = null;
 		   last_entry_ = null;
 	    }
    }
    
    
    public LogEntry createLogEntry() {

            int x = (min + (int)(Math.random() * ((max - min) + 1))) * 1000;
            int y = (min + (int)(Math.random() * ((max - min) + 1))) * 1000;
            
            assert(state_ == State.LEADER);
            
            writeRecord = new Record(x ,y);
            sum = x+y;
            
            return new LogEntry(leaderTerm, sum, writeRecord);
    }
    

	
	@SuppressWarnings("unused")
	private Node() throws RemoteException {super();}
	
	
	public int ComputeElectionTimeout(){
		    silencePeriod =  (min + (int)(Math.random() * ((max - min) + 10))) * 1000;
		    return silencePeriod;
	}

	public Node(String nameIn, String hostIn) throws RemoteException {
		
		name 			       = nameIn;
		host 			       = hostIn;
		leaderId               = null;
		state_ 		           = State.FOLLOWER;
		votedCurrTerm          = currentTerm  = 0;
		repl_                  = new Repl_TwoPhaseCommit();	
		localLog               = new Vector<LogEntry>();
		messagePeriod          = 4000;
		silencePeriod          = ComputeElectionTimeout();
		timer_                 = new RaftTimer(); 
		if(stateLog == null) 
          	stateLog = new Vector<LogEntry>();
			
	}

	class RaftTimer extends Timer {

		    private RaftElectionTask electionTask_ = null;
		    private AppendEntryTask appendEntryTask_ = null;
		    
			public RaftTimer() {
				appendEntryTask_ = new AppendEntryTask();
				schedule(appendEntryTask_, delay, messagePeriod); 
			}
	}
	
	class RaftElectionTask extends TimerTask {
		
			public void run(){
				
				StateTrace(state_,"Inside Election task.....");
				
				try {
					
					votedCurrTerm = recvVoteCount = 0;

					if (state_.equals(State.CANDIDATE)) { 
						        
						        Registry reg = LocateRegistry.getRegistry();
						        
								currentTerm++;
								
								votedCurrTerm++;     // vote Flag
								
								recvVoteCount++;     // self-ballot
								
								StateTrace(state_, "log size :" + localLog.size());
						
								StateTrace(state_, "Start Election : Req votes. election term is :"+ currentTerm);
						        
								for (String nodeName : reg.list()) {
		                                if (nodeName == name)
		                                	continue;
										RaftRequestInterface otherNodeStub = (RaftRequestInterface) reg.lookup(nodeName);
										int voteReply = otherNodeStub.voteRequest(currentTerm , name , repl_.last_entry_, repl_.local_last_index_);
										if (voteReply > 0)
											recvVoteCount++ ;
									
								}
								
								StateTrace(state_, "Start Election : Got votes for election term is :" + recvVoteCount);
								
								if(recvVoteCount >=  ((MaxNodes/2) + 1)) {
						                                    
									
										StateTrace(state_, "new leader for term " + currentTerm);
										state_    = State.LEADER;	
										leaderId  = name ;
										leaderTerm = currentTerm;
									    timer_.electionTask_.cancel(); // election over
									    
										for (String nodeName : reg.list()) {
											  // if (0 == nodeName.compareTo(name))
				                              //  	continue;
											 if(!(nodeName.equals(leaderId))){
											    RaftRequestInterface otherNodeStub = (RaftRequestInterface) reg.lookup(nodeName);
											    appendEntryCount+= otherNodeStub.appendEntryRequest(null, leaderTerm, leaderId, repl_.last_entry_, repl_.local_last_index_);}
										}

										if (appendEntryCount < ((MaxNodes/2) + 1)) 
										    StateTrace(state_, "Append Entry Majority Fail: Do nothing or assert"); 
								
									}
								
								//reset
								votedCurrTerm = 0;
								appendEntryCount = 0;
						}
 			
				    } catch (RemoteException | NotBoundException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
				}
			}
}
	
	class AppendEntryTask extends TimerTask {
		
		public void run(){
			
		    try {
			     	Registry reg = LocateRegistry.getRegistry(host);
			
			     	switch (state_) {
			     	
			     	case LEADER :
			     	    // race possible with remote append entry request calls 
				        if (leaderTerm < currentTerm) {
					       state_ = State.FOLLOWER;
				           break;
				        }
					    
				        //still leader
				        
				        if (hb < TEST001) {   
							
				            repl_.flush_entry_  = createLogEntry();
				            appendEntryCount = 0;
				            
							for (String nodeName : reg.list()) {
								 if (0 == nodeName.compareTo(name))
                                	continue;
								 RaftRequestInterface otherNodeStub = (RaftRequestInterface) reg.lookup(nodeName);
								 if (otherNodeStub.appendEntryRequest(repl_.flush_entry_, leaderTerm, leaderId, repl_.last_entry_, repl_.local_last_index_) > 0)
									appendEntryCount++;
							}								
					          
							// Leader Committing Results
					      	if(appendEntryCount >= (MaxNodes/2) +1){
			
								stateLog.addElement(repl_.flush_entry_);
								localLog.addElement(repl_.flush_entry_);
								repl_.last_entry_ =  repl_.flush_entry_;
								repl_.local_last_index_ = ++repl_.state_last_index_;
								repl_.flush_entry_ = null;
								ReplTrace(state_, "Entry Committed : Term " + repl_.last_entry_.term + " index " + repl_.local_last_index_);
								
					        } else {
					        	
					        	StateTrace(state_, "TBD : Do Nothing");
					        	
					        }
					      	
					      	appendEntryCount = 0;
					        hb++;
					        
					        // Trigger re-election
				        } else {
				        	
                            hb = 0;
				      }
				      break;
				      
				      
			     	case CANDIDATE :
			     		
						if (heardFromLeader) {
							timer_.electionTask_.cancel();
							heardFromLeader = false;
						    state_ = State.FOLLOWER;
						    StateTrace(state_," stepped down.");
						   
						} 
				        break;
				        
			     	case  FOLLOWER:
			     		
						if (heardFromLeader) {
							heardFromLeader = false;
						} else {
							state_ = State.CANDIDATE;
							StateTrace(state_, name + " No Hearbeat from leader of term " + currentTerm);
							timer_.electionTask_    = new RaftElectionTask();
							timer_.schedule(timer_.electionTask_, 0, silencePeriod);
						}
						break;
						
					default :
						break;
			}
						
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
	}
	
	
	public int appendEntryRequest(LogEntry newLogEntry,
			                     int leaderTerm, 
			                     String leaderName, 
			                     LogEntry lastCommitLogEntry,
			                     int lastCommitedLogIndex) 
			                     throws RemoteException {
		
		
		
		if (state_ == State.LEADER) {	
		    StateTrace(state_, " " + leaderName);
			//throw new RemoteException();
		    return 0;
		}	
		
		StateTrace(state_, "Append Entry Argument Index " + lastCommitedLogIndex);
		
		// only for rouge leader
		if (currentTerm > leaderTerm) {
			
			StateTrace (state_, " stale append entry req from id "+ leaderName + " current term :" + currentTerm + " leader term :" + leaderTerm);
			return 0;
			
		} else  if (currentTerm < leaderTerm) {
			
				if (state_ == State.LEADER)
					throw new RemoteException();
				
				heardFromLeader = true;
				currentTerm = leaderTerm;
				
				if (repl_.flush_entry_ == null) {
					repl_.flush_entry_ = newLogEntry;
					StateTrace (state_, " null append entry req from id "+ leaderName + " leader term :" + leaderTerm);
					return 1;
			
				} else { 
					
					if (lastCommitedLogIndex > repl_.local_last_index_) {
						localLog.addElement(repl_.flush_entry_);
						repl_.last_entry_ =  repl_.flush_entry_;
						repl_.local_last_index_ = lastCommitedLogIndex; //Note
						repl_.flush_entry_ = newLogEntry;
					    ReplTrace(state_, "Entry Committed : Term " + repl_.last_entry_.term + " index " + repl_.local_last_index_);
						return 1;
						
					} else if (newLogEntry == null && lastCommitedLogIndex == repl_.local_last_index_ ){
						repl_.flush_entry_ = null;
						ReplTrace(state_, "New Leader heartbeat: "+leaderName +"discard last flush entry" );
						return 0;
					}else if (newLogEntry == null && lastCommitedLogIndex > repl_.local_last_index_ ){	
						localLog.addElement(repl_.flush_entry_);
						repl_.last_entry_ =  repl_.flush_entry_;
						repl_.local_last_index_ = lastCommitedLogIndex; //Note
						repl_.flush_entry_ = newLogEntry;
					    ReplTrace(state_, "Entry Committed : Term " + repl_.last_entry_.term + " index " + repl_.local_last_index_);
					    return 1;
						
				     }else  {
								
						ReplTrace(state_, "TBD Case");
						return 0;
					} 
									
			    }
				
					
		} else {	
			
			if (state_ == State.LEADER)
				throw new RemoteException();
			
			if (repl_.local_last_index_ > lastCommitedLogIndex) {
				StateTrace (state_, " stale append entry req from id "+ leaderName + " local Last index :" + repl_.local_last_index_  + " leader last term :" + lastCommitedLogIndex);
				return 0;
			
			} else { 	
			
				 // Genuine-Leader
				
				heardFromLeader = true;
				StateTrace(state_, "append entry request received from leader id " + leaderName);
				
				// New Leader
				if (newLogEntry == null) {
					 heardFromLeader = true;
					 StateTrace (state_, " null append entry req from id "+ leaderName + " leader term :" + leaderTerm);
				     return 1;
				     
				} else {
				    // First Entry for a 2 Phase Commit
					if (repl_.flush_entry_ == null)  {
						assert(lastCommitedLogIndex == -1);
						repl_.flush_entry_ = newLogEntry;
						return 1;
					}	
					
					if (null == lastCommitLogEntry) {
						//over-write : 2 phase commit
					    repl_.flush_entry_ = newLogEntry;
						return 1;
					}
						
					// Level 1 check for term
					if (repl_.flush_entry_.term <= lastCommitLogEntry.term)  {
						
						// level 2 check for Log index
						
						if (repl_.local_last_index_ > lastCommitedLogIndex) {
							ReplTrace(state_, "ignore app entry request 001");
							return 0;
						
						} else if (repl_.local_last_index_ < lastCommitedLogIndex) { 	
			
							localLog.addElement(repl_.flush_entry_);
							repl_.last_entry_ =  repl_.flush_entry_;
							repl_.local_last_index_++;
							assert (repl_.local_last_index_ == lastCommitedLogIndex);
							repl_.flush_entry_ = newLogEntry;
							ReplTrace(state_, "Entry Committed " + repl_.last_entry_.term + "index " + repl_.local_last_index_);
				            return 1;
						
						} else {
							ReplTrace(state_, "ignore app entry request 002");
							assert(false);
						}
						
					} else if (repl_.flush_entry_.term > lastCommitLogEntry.term) {
						ReplTrace(state_, "ignore app entry request 003 ");
						return 0;
					/*	localLog.addElement(repl_.flush_entry_);
						repl_.last_entry_ =  repl_.flush_entry_;
						repl_.local_last_index_++;
						assert (repl_.local_last_index_ == lastCommitedLogIndex);
						repl_.flush_entry_ = newLogEntry;
						ReplTrace(state_, "Entry Committed " + repl_.last_entry_.term + "index " + repl_.local_last_index_);
			            return 1;  */
						
					} 
				}
		    }
		}
		return 0;
	}			

      

	@Override
	public int voteRequest(int newTerm, 
			               String candidateName, 
			               LogEntry lastCandidateCommitedEntry,
			               int lastCandidateCommitedIndex) 
			               throws RemoteException {
		
		
        if (newTerm < currentTerm || (newTerm == currentTerm && repl_.local_last_index_ > lastCandidateCommitedIndex) || votedCurrTerm != 0) {
					StateTrace(state_, "\n Vote denied by " + name + "\t to Candidate "+ candidateName +"\t for term "+ newTerm);
			        return 0;
		} else if(newTerm > leaderTerm && (repl_.local_last_index_ == lastCandidateCommitedIndex) && (name.equals(leaderId))){
			StateTrace(state_," stepped down.");
			state_ = State.FOLLOWER;
			currentTerm  = newTerm;
		    votedCurrTerm ++;
		    StateTrace(state_, "\n Vote given by " + name + "\t to Candidate "+ candidateName +"\t for term "+ newTerm);
	        return 1;
	    }else{  
			      currentTerm  = newTerm;
			      votedCurrTerm ++;
			      StateTrace(state_, "\n Vote given by " + name + "\t to Candidate "+ candidateName +"\t for term "+ newTerm);
		          return 1;
		}
	}
	
	public static void main(String[] args) {
		
		        String name = "Node-" + System.currentTimeMillis();
		        
			    String hostname = (args.length < 2) ? null : args[1];
				try {
					   Node class_obj = new Node(name,hostname);
					   Registry registry = LocateRegistry.getRegistry();
					   RaftRequestInterface server_if = (RaftRequestInterface) UnicastRemoteObject.exportObject(class_obj, 0);
                      
					   registry.bind(name, server_if);
				   //  System.out.println("RMI_Hello Server is ready." + name);
				
				} catch (Exception e) {
					System.out.println("Node Error: " + e.toString());
					e.printStackTrace();
				}
	}
	
}
