package gossip;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Random;

/*
Highly dependent of randomize
Workaround: Weight the seeds
*/
public class GossipManager extends Thread {
  List<Member> members;
  
  public int interval = 250;
  int iterations = 20;
  int clusterSize = 200;
  int seedCount = 3;
  boolean failedConvergence = false;
  int totalGossips = 0;
  
  public GossipManager(int interval, int iterations, int clusterSize){
    this.interval = interval;
    this.iterations = iterations;
    this.clusterSize = clusterSize;
    
    members = new ArrayList<Member>();
   
    List<Member> seeds = new ArrayList<Member>();
    
    // Create seed list with initial seed members
    for(int i = 0; i < seedCount; i++){
      Member seedMember = new Member(i+1, interval,this);
      seeds.add(seedMember);
      members.add(seedMember);
    }
    for(int i = 0; i < seedCount; i++){
      seeds.get(i).setSeeds(seeds);
    }
    for(int i = 0; i < seedCount; i++){
      seeds.get(i).start();
    }
    // Seed setup complete
    
    //Spawn the remainder of the gossipers
    for(int i = seedCount; i < clusterSize; i++){
      Member member = new Member(i+1, interval, this);
      member.setSeeds(seeds);
      members.add(member);
      member.start();
    }
  }
  
  /*
   When a gossip member has converged (member list size == clusterSize)
   it calls back to let Gossip manager know.
   Gossip manager removes from members list and logs it
  */
  public void setConverged(Member member, boolean expired){
    if(!expired){
      List<Member> knownMembers = member.getMembers();
      Collections.sort(knownMembers, new Comparator<Member>(){
        public int compare(Member one, Member two){
         return one.getPeerId() - two.getPeerId();
        }
      });
      totalGossips+=member.getGossipCount();
      System.out.format("Peer %s member list size: %s, gossips: %s%n", 
        member.getPeerId(), knownMembers.size(), member.getGossipCount());
    }else{
      failedConvergence = true;
    }
    members.remove(member);
  }
  
  public boolean isConverged(){
    return members.size()==0;
  }
  
  public void run(){
    long start = System.currentTimeMillis();
    while(!isConverged()){
      try{
        Thread.sleep(100);
      }catch(Exception ex){
        ex.printStackTrace();
      }
    }
    long end = System.currentTimeMillis();
    if(!failedConvergence){
      System.out.format("Time to convergence: %f%n", (end-start)/1000.0);
    }
    System.out.format("Total Gossips %s, Avg gossips per member: %s%n", totalGossips, totalGossips/(float)clusterSize);
  }
  
  public static void main(String[] args){
    if(args.length != 3){
      System.out.println("Usage GossipManager <interval-ms> <max-iterations> <cluster-size>");
      System.exit(1);
    }
    new GossipManager(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
      Integer.parseInt(args[2])).start();
  } 
  
  
}

class Member extends Thread {

  int id;
  private long interval;
  private GossipManager manager;
  private List<Member> members;
  private Random rand;
  private int gossipCount = 0;
  private List<Integer> gossipList;
  private boolean converged = false;
  
  public Member( int id, long interval, GossipManager manager){
    this.id = id;
    this.interval = interval;
    this.manager = manager;
    rand = new Random(id * 31);
    gossipList = new ArrayList<Integer>();
  }
  
  public int getPeerId(){ return id; }
  
  public List<Member> getMembers(){ return members; }
  
  public int getGossipCount(){ return gossipCount; }
 
  public void setSeeds(List<Member> seedMembers){
    this.members = new ArrayList<Member>(seedMembers);
    members.add(this);
  }
  
  public void run(){
    try{
      for(int i = 0; i < manager.iterations; i++){
        Thread.sleep(interval);
        gossip();
        if(!converged && members.size() == manager.clusterSize ){
          converged = true;
          manager.setConverged(this, false);
        }
        if(i == manager.iterations -1 && !converged){
          System.out.format("FAILED Peer %s member list size: %s, gossips: %s%n", id, members.size(), gossipCount);
          manager.setConverged(this, true);
        }
        if(manager.isConverged()) break;
      }
    }catch(InterruptedException ex){
      ex.printStackTrace();
    }
  }
  
  protected void gossip(){
    int randomPeerId = rand.nextInt(members.size()-1);
    for(int i = 0; i < manager.clusterSize; i++){
      if(gossipList.contains(randomPeerId)){
        continue;
      }else{
        gossipList.add(randomPeerId);
        break;
      }
    }
    Member peer = members.get(randomPeerId);
    peer.sendMemberList(members);
  }
  
  public void sendMemberList(List<Member> peerMembers){
    List<Member> tmpList = new ArrayList<Member>(members);
    tmpList.addAll(peerMembers);
    HashSet<Member> memberSet = new HashSet<Member>(tmpList);
    Member[] memberArray = memberSet.toArray(new Member[0]);
    members = Arrays.asList(memberArray);
    gossipCount++;
  }
  
  @Override
  public String toString(){
    return "Peer-" + id;
  }
  
}
