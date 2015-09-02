/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.balancerv2;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.balancerv2.DispatcherV2.DDatanode;
import org.apache.hadoop.hdfs.server.balancerv2.DispatcherV2.DDatanode.StorageGroup;
import org.apache.hadoop.hdfs.server.balancerv2.DispatcherV2.Source;
import org.apache.hadoop.hdfs.server.balancerv2.DispatcherV2.Task;
import org.apache.hadoop.hdfs.server.balancerv2.DispatcherV2.Util;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

/** <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the 
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 * 
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold <threshold>]
 *      Example: bin/ start-balancer.sh 
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
 *               bin/ start-balancer.sh -idleiterations 20
 *                     start the balancer with maximum 20 consecutive idle iterations
 *               bin/ start-balancer.sh -idleiterations -1
 *                     run the balancer with default threshold infinitely
 * To stop:
 *      bin/ stop-balancer.sh
 * </pre>
 * 
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (1%, 100%) with a 
 * default value of 10%. The threshold sets a target for whether the cluster 
 * is balanced. A cluster is balanced if for each datanode, the utilization 
 * of the node (ratio of used space at the node to total capacity of the node) 
 * differs from the utilization of the (ratio of used space in the cluster 
 * to total capacity of the cluster) by no more than the threshold value. 
 * The smaller the threshold, the more balanced a cluster will become. 
 * It takes more time to run the balancer for small threshold values. 
 * Also for a very small threshold the cluster may not be able to reach the 
 * balanced state when applications write and delete files concurrently.
 * 
 * <p>The tool moves blocks from highly utilized datanodes to poorly 
 * utilized datanodes iteratively. In each iteration a datanode moves or 
 * receives no more than the lesser of 10G bytes or the threshold fraction 
 * of its capacity. Each iteration runs no more than 20 minutes.
 * At the end of each iteration, the balancer obtains updated datanodes
 * information from the namenode.
 * 
 * <p>A system property that limits the balancer's use of bandwidth is 
 * defined in the default configuration file:
 * <pre>
 * <property>
 *   <name>dfs.balance.bandwidthPerSec</name>
 *   <value>1048576</value>
 * <description>  Specifies the maximum bandwidth that each datanode 
 * can utilize for the balancing purpose in term of the number of bytes 
 * per second. </description>
 * </property>
 * </pre>
 * 
 * <p>This property determines the maximum speed at which a block will be 
 * moved from one datanode to another. The default value is 1MB/s. The higher 
 * the bandwidth, the faster a cluster can reach the balanced state, 
 * but with greater competition with application processes. If an 
 * administrator changes the value of this property in the configuration 
 * file, the change is observed when HDFS is next restarted.
 * 
 * <p>MONITERING BALANCER PROGRESS
 * <p>After the balancer is started, an output file name where the balancer 
 * progress will be recorded is printed on the screen.  The administrator 
 * can monitor the running of the balancer by reading the output file. 
 * The output shows the balancer's status iteration by iteration. In each 
 * iteration it prints the starting time, the iteration number, the total 
 * number of bytes that have been moved in the previous iterations, 
 * the total number of bytes that are left to move in order for the cluster 
 * to be balanced, and the number of bytes that are being moved in this 
 * iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left 
 * To Move" is decreasing.
 * 
 * <p>Running multiple instances of the balancer in an HDFS cluster is 
 * prohibited by the tool.
 * 
 * <p>The balancer automatically exits when any of the following five 
 * conditions is satisfied:
 * <ol>
 * <li>The cluster is balanced;
 * <li>No block can be moved;
 * <li>No block has been moved for specified consecutive iterations (5 by default);
 * <li>An IOException occurs while communicating with the namenode;
 * <li>Another balancer is running.
 * </ol>
 * 
 * <p>Upon exit, a balancer returns an exit code and prints one of the 
 * following messages to the output file in corresponding to the above exit 
 * reasons:
 * <ol>
 * <li>The cluster is balanced. Exiting
 * <li>No block can be moved. Exiting...
 * <li>No block has been moved for specified iterations (5 by default). Exiting...
 * <li>Received an IO exception: failure reason. Exiting...
 * <li>Another balancer is running. Exiting...
 * </ol>
 * 
 * <p>The administrator can interrupt the execution of the balancer at any 
 * time by running the command "stop-balancer.sh" on the machine where the 
 * balancer is running.
 */

@InterfaceAudience.Private
public class BalancerV2 {
  static final Log LOG = LogFactory.getLog(BalancerV2.class);

  static final Path BALANCER_ID_PATH = new Path("/system/balancer.id");

  private static final long GB = 1L << 30; //1GB
  private static final long MAX_SIZE_TO_MOVE = 10*GB;

  private static final String USAGE = "Usage: hdfs balancer"
      + "\n\t[-policy <policy>]\tthe balancing policy: "
      + BalancingPolicy.Node.INSTANCE.getName() + " or "
      + BalancingPolicy.Pool.INSTANCE.getName()
      + "\n\t[-threshold <threshold>]\tPercentage of disk capacity"
      + "\n\t[-exclude [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tExcludes the specified datanodes."
      + "\n\t[-include [-f <hosts-file> | <comma-separated list of hosts>]]"
      + "\tIncludes only the specified datanodes."
      + "\n\t[-idleiterations <idleiterations>]"
      + "\tNumber of consecutive idle iterations (-1 for Infinite) before exit.";
  
  private final DispatcherV2 dispatcher;
  private final BalancingPolicy policy;
  private final double threshold;
  private final double pairRatio;
  
  // all data node lists
  private final Collection<Source> overUtilized = new LinkedList<Source>();
  private final Collection<Source> aboveAvgUtilized = new LinkedList<Source>();
  private final Collection<StorageGroup> belowAvgUtilized
      = new LinkedList<StorageGroup>();
  private final Collection<StorageGroup> underUtilized
      = new LinkedList<StorageGroup>();

  /* Check that this Balancer is compatible with the Block Placement Policy
   * used by the Namenode.
   */
  private static void checkReplicationPolicyCompatibility(Configuration conf
      ) throws UnsupportedActionException {
    if (!(BlockPlacementPolicy.getInstance(conf, null, null, null) instanceof 
        BlockPlacementPolicyDefault)) {
      throw new UnsupportedActionException(
          "Balancer without BlockPlacementPolicyDefault");
    }
  }

  /**
   * Construct a balancer.
   * Initialize balancer. It sets the value of the threshold, and 
   * builds the communication proxies to
   * namenode as a client and a secondary namenode and retry proxies
   * when connection fails.
   */
  BalancerV2(NameNodeConnector theblockpool, Parameters p, Configuration conf) {
    final long movedWinWidth = conf.getLong(
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVEDWINWIDTH_DEFAULT);
    final int moverThreads = conf.getInt(
        DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_KEY,
        DFSConfigKeys.DFS_BALANCER_MOVERTHREADS_DEFAULT);
    final int dispatcherThreads = conf.getInt(
        DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_KEY,
        DFSConfigKeys.DFS_BALANCER_DISPATCHERTHREADS_DEFAULT);
    final int maxConcurrentMovesPerNode = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_KEY,
        DFSConfigKeys.DFS_DATANODE_BALANCE_MAX_NUM_CONCURRENT_MOVES_DEFAULT);
    
    this.dispatcher = new DispatcherV2(p.maxNoPedingMoveIterations, p.noPedingMoveSleepTime, p.blockByteNum,
    		p.maxIterationTime, theblockpool, p.nodesToBeIncluded,
        p.nodesToBeExcluded, movedWinWidth, moverThreads, dispatcherThreads,
        maxConcurrentMovesPerNode, conf);
    this.threshold = p.threshold;
    this.policy = p.policy;
    //new add
    this.pairRatio = p.pairRatio; 
  }
  
  private static long getCapacity(DatanodeStorageReport report, StorageType t) {
    long capacity = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        capacity += r.getCapacity();
      }
    }
    return capacity;
  }

  private static long getRemaining(DatanodeStorageReport report, StorageType t) {
    long remaining = 0L;
    for(StorageReport r : report.getStorageReports()) {
      if (r.getStorage().getStorageType() == t) {
        remaining += r.getRemaining();
      }
    }
    return remaining;
  }

  /**
   * Given a datanode storage set, build a network topology and decide
   * over-utilized storages, above average utilized storages, 
   * below average utilized storages, and underutilized storages. 
   * The input datanode storage set is shuffled in order to randomize
   * to the storage matching later on.
   *
   * @return the number of bytes needed to move in order to balance the cluster.
   */
  private long init(List<DatanodeStorageReport> reports) {
    // compute average utilization
    for (DatanodeStorageReport r : reports) {
      policy.accumulateSpaces(r);
    }
    policy.initAvgUtilization();

    // create network topology and classify utilization collections: 
    //   over-utilized, above-average, below-average and under-utilized.
    long overLoadedBytes = 0L, underLoadedBytes = 0L;
    for(DatanodeStorageReport r : reports) {
      final DDatanode dn = dispatcher.newDatanode(r.getDatanodeInfo());
      for(StorageType t : StorageType.getMovableTypes()) {
        final Double utilization = policy.getUtilization(r, t);
        if (utilization == null) { // datanode does not have such storage type 
          continue;
        }
        
        final long capacity = getCapacity(r, t);
        final double utilizationDiff = utilization - policy.getAvgUtilization(t);
        final double thresholdDiff = Math.abs(utilizationDiff) - threshold;
        final long maxSize2Move = computeMaxSize2Move(capacity,
            getRemaining(r, t), utilizationDiff, threshold);

        final StorageGroup g;
        long remainSize = 0;
        
        remainSize = getRemaining(r, t);
        if (utilizationDiff > 0) {
          final Source s = dn.addSource(t, maxSize2Move, dispatcher);
          s.setRemainSize(remainSize);
          
          if (thresholdDiff <= 0) { // within threshold
            aboveAvgUtilized.add(s);
          } else {
            overLoadedBytes += precentage2bytes(thresholdDiff, capacity);
            overUtilized.add(s);
          }
          g = s;
        } else {
          g = dn.addTarget(t, maxSize2Move);
          g.setRemainSize(remainSize);
          
          if (thresholdDiff <= 0) { // within threshold
            belowAvgUtilized.add(g);
          } else {
            underLoadedBytes += precentage2bytes(thresholdDiff, capacity);
            underUtilized.add(g);
          }
        }
        dispatcher.getStorageGroupMap().put(g);
      }
    }

    logUtilizationCollections();
    
    Preconditions.checkState(dispatcher.getStorageGroupMap().size()
        == overUtilized.size() + underUtilized.size() + aboveAvgUtilized.size()
           + belowAvgUtilized.size(),
        "Mismatched number of storage groups");
    
    // return number of bytes to be moved in order to make the cluster balanced
    return Math.max(overLoadedBytes, underLoadedBytes);
  }

  private static long computeMaxSize2Move(final long capacity, final long remaining,
      final double utilizationDiff, final double threshold) {
    final double diff = Math.min(threshold, Math.abs(utilizationDiff));
    long maxSizeToMove = precentage2bytes(diff, capacity);
    if (utilizationDiff < 0) {
      maxSizeToMove = Math.min(remaining, maxSizeToMove);
    }
    return Math.min(MAX_SIZE_TO_MOVE, maxSizeToMove);
  }

  private static long precentage2bytes(double precentage, long capacity) {
    Preconditions.checkArgument(precentage >= 0,
        "precentage = " + precentage + " < 0");
    return (long)(precentage * capacity / 100.0);
  }

  /* log the over utilized & under utilized nodes */
  private void logUtilizationCollections() {
    logUtilizationCollection("over-utilized", overUtilized);
    if (LOG.isTraceEnabled()) {
      logUtilizationCollection("above-average", aboveAvgUtilized);
      logUtilizationCollection("below-average", belowAvgUtilized);
    }
    logUtilizationCollection("underutilized", underUtilized);
  }

  private static <T extends StorageGroup>
      void logUtilizationCollection(String name, Collection<T> items) {
    LOG.info(items.size() + " " + name + ": " + items);
  }

  /**
   * Decide all <source, target> pairs and
   * the number of bytes to move from a source to a target
   * Maximum bytes to be moved per storage group is
   * min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
   * @return total number of bytes to move in this iteration
   */
  private long chooseStorageGroups() {
    // First, match nodes on the same node group if cluster is node group aware
    if (dispatcher.getCluster().isNodeGroupAware()) {
      chooseStorageGroups(Matcher.SAME_NODE_GROUP);
    }
    
    // Then, match nodes on the same rack
    chooseStorageGroups(Matcher.SAME_RACK);
    // At last, match all remaining nodes
    chooseStorageGroups(Matcher.ANY_OTHER);
    
    return dispatcher.bytesToMove();
  }

  /** Decide all <source, target> pairs according to the matcher. */
  private void chooseStorageGroups(final Matcher matcher) {
    /* first step: match each overUtilized datanode (source) to
     * one or more underUtilized datanodes (targets).
     */
    chooseStorageGroups(overUtilized, underUtilized, matcher);
    
    /* match each remaining overutilized datanode (source) to 
     * below average utilized datanodes (targets).
     * Note only overutilized datanodes that haven't had that max bytes to move
     * satisfied in step 1 are selected
     */
    chooseStorageGroups(overUtilized, belowAvgUtilized, matcher);

    /* match each remaining underutilized datanode (target) to 
     * above average utilized datanodes (source).
     * Note only underutilized datanodes that have not had that max bytes to
     * move satisfied in step 1 are selected.
     */
    chooseStorageGroups(underUtilized, aboveAvgUtilized, matcher);
  }

  /**
   * For each datanode, choose matching nodes from the candidates. Either the
   * datanodes or the candidates are source nodes with (utilization > Avg), and
   * the others are target nodes with (utilization < Avg).
   */
  private <G extends StorageGroup, C extends StorageGroup>
      void chooseStorageGroups(Collection<G> groups, Collection<C> candidates,
          Matcher matcher) {
	adjustTargetMaxSize2Move(groups, candidates);
	
    for(final Iterator<G> i = groups.iterator(); i.hasNext();) {
      final G g = i.next();
      for(; choose4One(g, candidates, matcher); );
      if (!g.hasSpaceForScheduling()) {
        i.remove();
      }
    }
  }
  
  //adjust maxSize2Move when targets'num more than src'num
  private <G extends StorageGroup, C extends StorageGroup>
  void adjustTargetMaxSize2Move(Collection<G> src, Collection<C> targets){
	  //adjust maxsize2Move
	  double ratio;
	  
	  LOG.info("src num is " + src.size() + ", target num is " + targets.size());
	  if(targets == null || targets.size() == 0 || src == null || src.size() == 0){
		  return;
	  }
	  
	  ratio = this.pairRatio;
	  LOG.info("input ratio is" + ratio);
	  if(src.size() > targets.size()){
		  if(ratio == -1){
			  ratio = src.size() /targets.size();  
		  }
		    
		  LOG.info("src num is " + src.size() + ", target num is " + targets.size() + ", adjust ratio is " + ratio);
		  
		  for(StorageGroup sg: targets){
			  LOG.info("targets begin size is " + sg.maxSize2Move + ", remainSize is " + sg.remainSize);
			  sg.adjustMaxSize(ratio);
			  LOG.info("targets adjust size is " + sg.maxSize2Move);
		  }
	  }else if (src.size() < targets.size()){
		  if(ratio == -1){
			  ratio = targets.size() / src.size();  
		  }
		    
		  LOG.info("src num is " + src.size() + "target num is " + targets.size() + "adjust ratio is " + ratio);
		  
		  for(StorageGroup sg: src){
			  LOG.info("src begin size is " + sg.maxSize2Move + ", remainSize is " + sg.remainSize);
			  sg.adjustMaxSize(ratio);
			  LOG.info("src adjust size is " + sg.maxSize2Move);
		  }
	  }
  }

  /**
   * For the given datanode, choose a candidate and then schedule it.
   * @return true if a candidate is chosen; false if no candidates is chosen.
   */
  private <C extends StorageGroup> boolean choose4One(StorageGroup g,
      Collection<C> candidates, Matcher matcher) {
    final Iterator<C> i = candidates.iterator();
    final C chosen = chooseCandidate(g, i, matcher);
  
    if (chosen == null) {
      return false;
    }
    if (g instanceof Source) {
      matchSourceWithTargetToMove((Source)g, chosen);
    } else {
      matchSourceWithTargetToMove((Source)chosen, g);
    }
    if (!chosen.hasSpaceForScheduling()) {
      i.remove();
    }
    return true;
  }
  
  private void matchSourceWithTargetToMove(Source source, StorageGroup target) {
    long size = Math.min(source.availableSizeToMove(), target.availableSizeToMove());
    final Task task = new Task(target, size);
    source.addTask(task);
    target.incScheduledSize(task.getSize());
    dispatcher.add(source, target);
    LOG.info("Decided to move "+StringUtils.byteDesc(size)+" bytes from "
        + source.getDisplayName() + " to " + target.getDisplayName());
  }
  
  /** Choose a candidate for the given datanode. */
  private <G extends StorageGroup, C extends StorageGroup>
      C chooseCandidate(G g, Iterator<C> candidates, Matcher matcher) {
    if (g.hasSpaceForScheduling()) {
      for(; candidates.hasNext(); ) {
        final C c = candidates.next();
        if (!c.hasSpaceForScheduling()) {
          candidates.remove();
        } else if (matcher.match(dispatcher.getCluster(),
            g.getDatanodeInfo(), c.getDatanodeInfo())) {
          return c;
        }
      }
    }
    return null;
  }

  /* reset all fields in a balancer preparing for the next iteration */
  void resetData(Configuration conf) {
    this.overUtilized.clear();
    this.aboveAvgUtilized.clear();
    this.belowAvgUtilized.clear();
    this.underUtilized.clear();
    this.policy.reset();
    dispatcher.reset(conf);;
  }

  static class Result {
    final ExitStatus exitStatus;
    final long bytesLeftToMove;
    final long bytesBeingMoved;
    final long bytesAlreadyMoved;

    Result(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved,
        long bytesAlreadyMoved) {
      this.exitStatus = exitStatus;
      this.bytesLeftToMove = bytesLeftToMove;
      this.bytesBeingMoved = bytesBeingMoved;
      this.bytesAlreadyMoved = bytesAlreadyMoved;
    }

    void print(int iteration, PrintStream out) {
      out.printf("%-24s %10d  %19s  %18s  %17s%n",
          DateFormat.getDateTimeInstance().format(new Date()), iteration,
          StringUtils.byteDesc(bytesAlreadyMoved),
          StringUtils.byteDesc(bytesLeftToMove),
          StringUtils.byteDesc(bytesBeingMoved));
    }
  }

  Result newResult(ExitStatus exitStatus, long bytesLeftToMove, long bytesBeingMoved) {
    return new Result(exitStatus, bytesLeftToMove, bytesBeingMoved,
        dispatcher.getBytesMoved());
  }

  Result newResult(ExitStatus exitStatus) {
    return new Result(exitStatus, -1, -1, dispatcher.getBytesMoved());
  }

  protected static long runOneIterationBytesNum = 0;
  
  /** Run an iteration for all datanodes. */
  Result runOneIteration() {
	 runOneIterationBytesNum = 0;
    try {
      long startTime = System.currentTimeMillis();
      final List<DatanodeStorageReport> reports = dispatcher.init();
      final long bytesLeftToMove = init(reports);
      if (bytesLeftToMove == 0) {
        System.out.println("The cluster is balanced. Exiting...");
        return newResult(ExitStatus.SUCCESS, bytesLeftToMove, -1);
      } else {
        LOG.info( "Need to move "+ StringUtils.byteDesc(bytesLeftToMove)
            + " to make the cluster balanced." );
      }
      
      long endTime1 = System.currentTimeMillis();
      LOG.info(" decide over-utilized storages, above average utilized storages, below average utilized storages, and underutilized storages elapse time: " + (endTime1 - startTime));
      /* Decide all the nodes that will participate in the block move and
       * the number of bytes that need to be moved from one node to another
       * in this iteration. Maximum bytes to be moved per node is
       * Min(1 Band worth of bytes,  MAX_SIZE_TO_MOVE).
       */
      final long bytesBeingMoved = chooseStorageGroups();
      if (bytesBeingMoved == 0) {
        System.out.println("No block can be moved. Exiting...");
        return newResult(ExitStatus.NO_MOVE_BLOCK, bytesLeftToMove, bytesBeingMoved);
      } else {
        LOG.info( "Will move " + StringUtils.byteDesc(bytesBeingMoved) +
            " in this iteration");
      }
      
      long endTime2 = System.currentTimeMillis();
      LOG.info(" decide source-target storages elapse time: " + (endTime2 - endTime1));
      /* For each pair of <source, target>, start a thread that repeatedly 
       * decide a block to be moved and its proxy source, 
       * then initiates the move until all bytes are moved or no more block
       * available to move.
       * Exit no byte has been moved for 5 consecutive iterations.
       */
      if (!dispatcher.dispatchAndCheckContinue()) {
        return newResult(ExitStatus.NO_MOVE_PROGRESS, bytesLeftToMove, bytesBeingMoved);
      }
      long endTime3 = System.currentTimeMillis();
      long elapseTime = (endTime3 - endTime2)/1000;
      String message = "";
      
      if(elapseTime > 0) {
    	  message = ", bytesPerSecond: " + runOneIterationBytesNum/elapseTime;
      }
      LOG.info(" choose blocks and copy blocks elapse time: " + elapseTime + message);
      runOneIterationBytesNum = 0;
      return newResult(ExitStatus.IN_PROGRESS, bytesLeftToMove, bytesBeingMoved);
    } catch (IllegalArgumentException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.ILLEGAL_ARGUMENTS);
    } catch (IOException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.IO_EXCEPTION);
    } catch (InterruptedException e) {
      System.out.println(e + ".  Exiting ...");
      return newResult(ExitStatus.INTERRUPTED);
    } finally {
      dispatcher.shutdownNow();
    }
  }

  /**
   * Balance all namenodes.
   * For each iteration,
   * for each namenode,
   * execute a {@link BalancerV2} to work through all datanodes once.  
   */
  static int run(Collection<URI> namenodes, final Parameters p,
      Configuration conf) throws IOException, InterruptedException {
    final long sleeptime =
        conf.getLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
            DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 2000 +
        conf.getLong(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_DEFAULT) * 1000;
    LOG.info("namenodes  = " + namenodes);
    LOG.info("parameters = " + p);
    
    System.out.println("Time Stamp               Iteration#  Bytes Already Moved  Bytes Left To Move  Bytes Being Moved");
    
    List<NameNodeConnector> connectors = Collections.emptyList();
    try {
      connectors = NameNodeConnector.newNameNodeConnectors(namenodes, 
            BalancerV2.class.getSimpleName(), BALANCER_ID_PATH, conf, p.maxIdleIteration);
    
      boolean done = false;
      for(int iteration = 0; !done; iteration++) {
        done = true;
        Collections.shuffle(connectors);
        for(NameNodeConnector nnc : connectors) {
        	
          final BalancerV2 b = new BalancerV2(nnc, p, conf);
          final Result r = b.runOneIteration();
          r.print(iteration, System.out);

          // clean all lists
          b.resetData(conf);
          if (r.exitStatus == ExitStatus.IN_PROGRESS) {
            done = false;
          } else if (r.exitStatus != ExitStatus.SUCCESS) {
            //must be an error statue, return.
            return r.exitStatus.getExitCode();
          }
        }

        if (!done) {
          Thread.sleep(sleeptime);
        }
      }
    } finally {
      for(NameNodeConnector nnc : connectors) {
    	LOG.info("BalancerV2.run stop balancer");
        IOUtils.cleanup(LOG, nnc);
      }
    }
    return ExitStatus.SUCCESS.getExitCode();
  }

  /* Given elaspedTime in ms, return a printable string */
  private static String time2Str(long elapsedTime) {
    String unit;
    double time = elapsedTime;
    if (elapsedTime < 1000) {
      unit = "milliseconds";
    } else if (elapsedTime < 60*1000) {
      unit = "seconds";
      time = time/1000;
    } else if (elapsedTime < 3600*1000) {
      unit = "minutes";
      time = time/(60*1000);
    } else {
      unit = "hours";
      time = time/(3600*1000);
    }

    return time+" "+unit;
  }

  static class Parameters {
    static final Parameters DEFAULT = new Parameters(100, 1000, 1000 * 60 *20, 1,
        BalancingPolicy.Node.INSTANCE, 10.0, 
        NameNodeConnector.DEFAULT_MAX_IDLE_ITERATIONS, 1.0,
        Collections.<String> emptySet(), Collections.<String> emptySet());

    final BalancingPolicy policy;
    final double threshold;
    final long blockByteNum;
    final int maxIdleIteration;
    final long maxIterationTime;
    final long maxNoPedingMoveIterations;
    final long noPedingMoveSleepTime;
    final double pairRatio;
    // exclude the nodes in this set from balancing operations
    Set<String> nodesToBeExcluded;
    //include only these nodes in balancing operations
    Set<String> nodesToBeIncluded;

    Parameters(long maxNoPedingMoveIterations, long noPedingMoveSleepTime, 
    		long maxIterationTime, long blockByteNum, BalancingPolicy policy, double threshold, int maxIdleIteration, double pairRatio,
        Set<String> nodesToBeExcluded, Set<String> nodesToBeIncluded) {
      this.policy = policy;
      this.threshold = threshold;
      this.maxIdleIteration = maxIdleIteration;
      this.nodesToBeExcluded = nodesToBeExcluded;
      this.nodesToBeIncluded = nodesToBeIncluded;
      
      //new add
      this.blockByteNum = blockByteNum;
      this.maxIterationTime = maxIterationTime;
      this.maxNoPedingMoveIterations = maxNoPedingMoveIterations;
      this.noPedingMoveSleepTime = noPedingMoveSleepTime;
      this.pairRatio = pairRatio;
    }

    @Override
    public String toString() {
      return BalancerV2.class.getSimpleName() + "." + getClass().getSimpleName()
          + "[" + policy + ", threshold=" + threshold +
          ", max idle iteration = " + maxIdleIteration +
          ", pairRatio = " + pairRatio +
          ", number of nodes to be excluded = "+ nodesToBeExcluded.size() +
          ", number of nodes to be included = "+ nodesToBeIncluded.size() +"]";
    }
  }

  static class Cli extends Configured implements Tool {
    /**
     * Parse arguments and then run Balancer.
     * 
     * @param args command specific arguments.
     * @return exit code. 0 indicates success, non-zero indicates failure.
     */
    @Override
    public int run(String[] args) {
      final long startTime = Time.monotonicNow();
      final Configuration conf = getConf();

      try {
        checkReplicationPolicyCompatibility(conf);

        final Collection<URI> namenodes = DFSUtil.getNsServiceRpcUris(conf);
        return BalancerV2.run(namenodes, parse(args), conf);
      } catch (IOException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.IO_EXCEPTION.getExitCode();
      } catch (InterruptedException e) {
        System.out.println(e + ".  Exiting ...");
        return ExitStatus.INTERRUPTED.getExitCode();
      } finally {
        System.out.format("%-24s ",
            DateFormat.getDateTimeInstance().format(new Date()));
        System.out.println("Balancing took "
            + time2Str(Time.monotonicNow() - startTime));
      }
    }

    /** parse command line arguments */
    static Parameters parse(String[] args) {
      BalancingPolicy policy = Parameters.DEFAULT.policy;
      double threshold = Parameters.DEFAULT.threshold;
      long blockBytesNum = 1;
      long maxIterationTime = 20 * 60 * 1000L; // 20 mins
      long maxNoPedingMoveIterations = 100;
      long noPedingMoveSleepTime = 1000;
      int maxIdleIteration = Parameters.DEFAULT.maxIdleIteration;
      double pairRatio = 1;
      Set<String> nodesTobeExcluded = Parameters.DEFAULT.nodesToBeExcluded;
      Set<String> nodesTobeIncluded = Parameters.DEFAULT.nodesToBeIncluded;

      if (args != null) {
        try {
          for(int i = 0; i < args.length; i++) {
        	if("-maxNoPedingMoveIterations".equalsIgnoreCase(args[i])) {
          		checkArgument(++i < args.length,
                          "maxNoPedingMoveIterations value is missing: args = " + Arrays.toString(args));
          		try{
          			maxNoPedingMoveIterations = Long.parseLong(args[i]);
              		LOG.info( "Using a maxNoPedingMoveIterations of " + maxNoPedingMoveIterations );
              	} catch(IllegalArgumentException e) {
              		System.err.println("Illegal maxNoPedingMoveIterations" + args[i]);
              		throw e;
              	}
          	} else  if("-noPedingMoveSleepTime".equalsIgnoreCase(args[i])) {
          		checkArgument(++i < args.length,
                          "noPedingMoveSleepTime value is missing: args = " + Arrays.toString(args));
          		try{
          			noPedingMoveSleepTime = Long.parseLong(args[i]);
              		LOG.info( "Using a noPedingMoveSleepTime of " + noPedingMoveSleepTime );
              	} catch(IllegalArgumentException e) {
              		System.err.println("Illegal noPedingMoveSleepTime" + args[i]);
              		throw e;
              	}
          	} else if("-maxIterationTime".equalsIgnoreCase(args[i])) {
        		checkArgument(++i < args.length,
                        "maxIterationTime value is missing: args = " + Arrays.toString(args));
        		try{
        			maxIterationTime = Long.parseLong(args[i]);
            		LOG.info( "Using a maxIterationTime of " + maxIterationTime );
            	} catch(IllegalArgumentException e) {
            		System.err.println("Illegal maxIterationTime" + args[i]);
            		throw e;
            	}
        	} else if("-pairRatio".equalsIgnoreCase(args[i])) {
        		checkArgument(++i < args.length,
                        "-pairRatio value is missing: args = " + Arrays.toString(args));
        		try{
        			pairRatio = Long.parseLong(args[i]);
            		LOG.info( "Using a pairRatio of " + pairRatio );
            	} catch(IllegalArgumentException e) {
            		System.err.println("Illegal pairRatio" + args[i]);
            		throw e;
            	}
        	} else if ("-threshold".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "Threshold value is missing: args = " + Arrays.toString(args));
              try {
                threshold = Double.parseDouble(args[i]);
                if (threshold < 1 || threshold > 100) {
                  throw new IllegalArgumentException(
                      "Number out of range: threshold = " + threshold);
                }
                LOG.info( "Using a threshold of " + threshold );
              } catch(IllegalArgumentException e) {
                System.err.println(
                    "Expecting a number in the range of [1.0, 100.0]: "
                    + args[i]);
                throw e;
              }
            } else if ("-blockBytesNum".equalsIgnoreCase(args[i])) {
            	checkArgument(++i < args.length,
                        "blockBytesNum value is missing: args = " + Arrays.toString(args));
            	try{
            		blockBytesNum = Long.parseLong(args[i]);
            		LOG.info( "Using a blockBytesNum of " + blockBytesNum );
            	} catch(IllegalArgumentException e) {
            		System.err.println("Illegal blockBytesNum" + args[i]);
            		throw e;
            	}
            	
            }
            
            else if ("-policy".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "Policy value is missing: args = " + Arrays.toString(args));
              try {
                policy = BalancingPolicy.parse(args[i]);
              } catch(IllegalArgumentException e) {
                System.err.println("Illegal policy name: " + args[i]);
                throw e;
              }
            } else if ("-exclude".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "List of nodes to exclude | -f <filename> is missing: args = "
                  + Arrays.toString(args));
              if ("-f".equalsIgnoreCase(args[i])) {
                checkArgument(++i < args.length,
                    "File containing nodes to exclude is not specified: args = "
                    + Arrays.toString(args));
                nodesTobeExcluded = Util.getHostListFromFile(args[i], "exclude");
              } else {
                nodesTobeExcluded = Util.parseHostList(args[i]);
              }
            } else if ("-include".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                "List of nodes to include | -f <filename> is missing: args = "
                + Arrays.toString(args));
              if ("-f".equalsIgnoreCase(args[i])) {
                checkArgument(++i < args.length,
                    "File containing nodes to include is not specified: args = "
                    + Arrays.toString(args));
                nodesTobeIncluded = Util.getHostListFromFile(args[i], "include");
               } else {
                nodesTobeIncluded = Util.parseHostList(args[i]);
              }
            } else if ("-idleiterations".equalsIgnoreCase(args[i])) {
              checkArgument(++i < args.length,
                  "idleiterations value is missing: args = " + Arrays.toString(args));
              maxIdleIteration = Integer.parseInt(args[i]);
              LOG.info("Using a idleiterations of " + maxIdleIteration);
            } else {
              throw new IllegalArgumentException("args = "
                  + Arrays.toString(args));
            }
          }
          checkArgument(nodesTobeExcluded.isEmpty() || nodesTobeIncluded.isEmpty(),
              "-exclude and -include options cannot be specified together.");
        } catch(RuntimeException e) {
          printUsage(System.err);
          throw e;
        }
      }
      
      return new Parameters(maxNoPedingMoveIterations, noPedingMoveSleepTime, maxIterationTime, blockBytesNum, policy, threshold, maxIdleIteration, pairRatio, nodesTobeExcluded, nodesTobeIncluded);
    }

    private static void printUsage(PrintStream out) {
      out.println(USAGE + "\n");
    }
  }

  /**
   * Run a balancer
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    if (DFSUtil.parseHelpArgument(args, USAGE, System.out, true)) {
      System.exit(0);
    }

    try {
      System.exit(ToolRunner.run(new HdfsConfiguration(), new Cli(), args));
    } catch (Throwable e) {
      LOG.error("Exiting balancer due an exception", e);
      System.exit(-1);
    }
  }
}
