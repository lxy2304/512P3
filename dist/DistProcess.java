/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2024, Bettina Kemme
*/
import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your manager process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your manager's logic and worker's logic.
//		This is important as both the manager and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For simplicity, so far all the code in a single class (including the callbacks).
//		You are free to break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! Managers and Workers are also clients of ZK and the ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		In particular, if the process is a worker, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback
{
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isManager=false;
	boolean initialized=false;

	Queue<String> available_workers;
	Set<String> all_tasks;
	Queue<String> pending_tasks;
	private final ExecutorService workerPool = Executors.newCachedThreadPool(); // Dynamic thread pool
	DistProcess(String zkhost)
	{
		zkServer=zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
	}

	void initialize()
	{
		try
		{
			runForManager();	// See if you can become the manager (i.e, no other manager exists)
			isManager=true;
//			getTasks(); // Install monitoring on any new tasks that will be created.
		}catch(NodeExistsException nee)
		{
			isManager=false;
			try {
				runForWorker();
			}catch (Exception e) {
				System.out.println(e);
			}
		}
		catch(UnknownHostException uhe)
		{ System.out.println(uhe); }
		catch(KeeperException ke)
		{ System.out.println(ke); }
		catch(InterruptedException ie)
		{ System.out.println(ie); }

		System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isManager?"manager":"worker"));

	}

	// Manager fetching task znodes...
	void getTasks()
	{
		zk.getChildren("/dist23/tasks", this, this, null);
	}

	// Try to become the manager.
	void runForManager() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved
        try {
            zk.create("/dist23", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
			//do nothing
		}
        zk.create("/dist23/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.addWatch("/dist23/manager", this, AddWatchMode.PERSISTENT);
		zk.create("/dist23/workers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.create("/dist23/tasks",null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.addWatch("/dist23/tasks", this, AddWatchMode.PERSISTENT);

		available_workers = new LinkedList<>();
		pending_tasks = new LinkedList<>();
		all_tasks = new HashSet<>();
	}

	void runForWorker() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist23/workers/worker-" + pinfo, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.addWatch("/dist23/workers/worker-" + pinfo, this, AddWatchMode.PERSISTENT);
		zk.create("/dist23/manager/worker-" + pinfo, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}


	public void process(WatchedEvent e)
	{
		//Get watcher notifications.

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		System.out.println("DISTAPP : Event received : " + e);

		if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
		{
			// Once we are connected, do our intialization stuff.
			if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initialized == false) 
			{
				initialize();
				initialized = true;
			}
		}

		// Manager should be notified if any new znodes are added to tasks.
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist23/tasks"))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getTasks();
		}

		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist23/manager"))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getAvailableWorkers();
		}

		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist23/workers/worker-" + pinfo))
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			getAssignedTask();
		}
	}

	void getAssignedTask() {
		zk.getChildren("/dist23/workers/worker-" + pinfo, this, this, null);
	}

	void getAvailableWorkers() {
		zk.getChildren("/dist23/manager", this, this, null);
	}

	void processTask(byte[] taskSerial, String task) {
		workerPool.submit(() -> { // Submit task to the thread pool
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
				ObjectInput in = new ObjectInputStream(bis);
				DistTask dt = (DistTask) in.readObject();

				dt.compute(); // Perform time-consuming computation

				// Serialize result and update ZooKeeper
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(dt);
				oos.flush();
				byte[] taskResult = bos.toByteArray();

				zk.create("/dist23/tasks/" + task + "/result", taskResult, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				zk.create("/dist23/manager/worker-" + pinfo, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				zk.delete("/dist23/workers/worker-" + pinfo +"/" + task, -1, null, null);
				System.out.println("Task " + task + " completed by worker.");
			} catch (Exception e) {
				System.err.println("Error processing task: " + e.getMessage());
			}
		});
	}

	//Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{
		System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
		if (path.equals("/dist23/manager")) {
			for (String child: children) {
				zk.delete("/dist23/manager/" + child, -1, null, null);
				if (pending_tasks.size() != 0) {
					String task = pending_tasks.poll();
                    try {
                        zk.create("/dist23/workers/" + child + "/" + task, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    } catch (KeeperException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }else {
					available_workers.offer(child);
				}
			}
		}

		if (path.equals("/dist23/tasks")) {
			for (String task : children) {
				if (!all_tasks.contains(task)) {
					all_tasks.add(task);
					if (available_workers.size() != 0) {
						String worker = available_workers.poll();
						try {
							zk.create("/dist23/workers/" + worker + "/" + task, "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
							zk.delete("/dist23/manager/" + worker, -1, null, null);
						} catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (KeeperException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
						pending_tasks.offer(task);
					}
				} else {
					// old task... skipped
				}
			}
		}

		if (path.equals("/dist23/workers/worker-" + pinfo)) {
			if (children.size() > 0) {
				String task = children.get(0);
				zk.getData("/dist23/tasks/" + task, false, (rc1, path1, ctx1, data, stat) -> {
					if (rc1 == Code.OK.intValue()) {
	//						System.out.println("Task data retrieved for: " + path);
						processTask(data, task);
					} else {
						System.err.println("Error retrieving task data for: " + path1 + ". Code: " + rc1);
					}
				}, null);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// Create a new process
		// Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		// Set up a latch to keep the process running
		CountDownLatch latch = new CountDownLatch(1);
		AtomicBoolean isRunning = new AtomicBoolean(true);

		// Add a shutdown hook to handle graceful termination
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Shutting down process...");
			isRunning.set(false);
			latch.countDown(); // Release the latch
			try {
				if (dt.zk != null) {
					dt.zk.close(); // Close ZooKeeper connection
				}
			} catch (Exception e) {
				System.err.println("Error while closing ZooKeeper: " + e.getMessage());
			}
			System.out.println("Process terminated.");
		}));

		// Keep the main thread alive
		System.out.println("Process is up and running. Use 'kill' to terminate.");
		latch.await();
	}
}
