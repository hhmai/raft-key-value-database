# raft-key-value-database
# Approach
I approached the project by following the implementation steps. First, I made sure that my nodes could send messages to each other via broadcast.
I then implemented the election process and added state functionality to each node, making sure that I followed the RAFT election protocol. I then worked on partitioning which took the most work out of all other functionality. I had to implement steps to make sure that partitioned nodes were able to realize they were in a minority and act accordingly. I implemented timeout features for the leader node which would occur when it did not receive any messages from followers, meaning it was in the minority. 
# Challenges
Some of the biggest challenges I faced in this project was trying to implement the election process as stated in the RAFT protocol. It was hard to keep track of each node term and make sure it only increased when it was supposed to as outline. Another challenge I faced was fixing the partitioning problems as I was not able to fully implement hard partitioning procedures. I was able to bypass soft partitioning and made sure that the partitioned node was able to update its logs accordingly.
# List of Good Features
I think that some of the best implemented features I had in this code were the features that I followed the RAFT protocol exactly. For example, my election process was seamless and smooth, I found that there were never any unnecessary elections occurring.
# Tests
I tested my code through System Prints because I did not have access to debuggers or testing tools through the simulation. I did use the simulation configs to make sure that each functionality worked properly but utilized print statements to check each node state and ensure that it was working as intended. I tested my functionalities such as connecting and talking with simple configs, crashing nodes with the crashed configs, and partitioning with the corresponding configs.
