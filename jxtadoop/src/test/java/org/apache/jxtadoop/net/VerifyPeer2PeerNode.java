package org.apache.jxtadoop.net;

public class VerifyPeer2PeerNode {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Peer2PeerNode n0, n1, n2, n3, n4, n5, n6;
		Peer2peerTopology topology = new Peer2peerTopology();
		
		// Node 0 : In the main group
		 n0 = new Peer2PeerNode("/datanodes/09616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03");
		// Node 1 : In the main group
		 n1 = new Peer2PeerNode("19616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03","/datanodes/0");
		// Node 2 : In the broadcast domain #1		
		 n2 = new Peer2PeerNode("/datanodes/1/29616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03");
		// Node 3 : In the broadcast domain #1		
		 n3 = new Peer2PeerNode("/datanodes/1/39616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03");
		 // Node 4 : In the broadcast domain #2		
		 n4 = new Peer2PeerNode("/datanodes/2/49616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03");
		// Node 5 : In the broadcast domain #2		
		 n5 = new Peer2PeerNode("/datanodes/2/59616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03");
		 // Node 6 : Not in the topology
		 n6 = new Peer2PeerNode("/datanodes/3/69616261646162614E504720503250334BF8CE1FAFDDF001A3775FF8FB52662B03");
		
		 topology.add(n0);
		 topology.add(n1);
		 topology.add(n2);
		 topology.add(n3);
		 topology.add(n4);
		 topology.add(n5);
		 
		 // Test 1 : does topology contains a node
		 System.out.println("- Test 1");
		 System.out.println("Is n0 in topology ? "+topology.contains(n0));
		 System.out.println("Is n1 in topology ? "+topology.contains(n1));
		 System.out.println("Is n6 in topology ? "+topology.contains(n6));
		 
		 // Test 2 : removing & re-adding a node
		 System.out.println("`\n- Test 2");
		 System.out.println("Is n4 in topology ? "+topology.contains(n4));
		 topology.remove(n4);
		 System.out.println("Is n4 in topology ? "+topology.contains(n4));
		 topology.add(n4);
		 System.out.println("Is n4 in topology ? "+topology.contains(n4));
		 
		 // Test 3 : get number of racks & nodes
		 System.out.println("`\n- Test 3");
		 System.out.println("Number of domains : "+topology.getNumOfRacks());
		 System.out.println("Number of nodes : "+topology.getNumOfLeaves());
		 String [] domains = topology.getDomains();
		 System.out.println("Domains :");
		 for(int i=0; i < domains.length;i++) {
			 System.out.println("\t"+domains[i]);
		 }
		 
		 // Test 4 : Are the nodes on same rack ?
		 System.out.println("`\n- Test 4");
		 System.out.println("Are n0 and n1 together ? "+topology.isOnSameRack(n0, n1));
		 System.out.println("Are n1 and n2 together ? "+topology.isOnSameRack(n1, n2));
		 System.out.println("Are n2 and n3 together ? "+topology.isOnSameRack(n2, n3));
		 System.out.println("Are n3 and n4 together ? "+topology.isOnSameRack(n3, n4));
		 System.out.println("Are n4 and n5 together ? "+topology.isOnSameRack(n4, n5));
		 System.out.println("Are n5 and n0 together ? "+topology.isOnSameRack(n5, n0));
		 
		// Test 5 : Choose randon nodes
		 System.out.println("`\n- Test 5");
		 System.out.println("Random node in domain 0 : "+topology.chooseRandom("0").getName());
		 System.out.println("Random node in domain 1 : "+topology.chooseRandom("1").getName());
		 System.out.println("Random node in domain 2 : "+topology.chooseRandom("2").getName());
		 
		 // Test 6 : return distance between nodes
		 System.out.println("`\n- Test 6");
		 System.out.println("Distance between n0 and n1 : "+topology.getDistance(n0, n1));
		 System.out.println("Distance between n1 and n2 : "+topology.getDistance(n1, n2));
		 System.out.println("Distance between n2 and n3 : "+topology.getDistance(n2, n3));
		 System.out.println("Distance between n3 and n4 : "+topology.getDistance(n3, n4));
		 System.out.println("Distance between n4 and n5 : "+topology.getDistance(n4, n5));
		 System.out.println("Distance between n5 and n0 : "+topology.getDistance(n5, n0));
		 
		 
	}
}
