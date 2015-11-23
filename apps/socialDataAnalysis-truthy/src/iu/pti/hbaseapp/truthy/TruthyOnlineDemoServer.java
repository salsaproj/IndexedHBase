package iu.pti.hbaseapp.truthy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;
import com.google.gson.JsonArray;

public class TruthyOnlineDemoServer {
	private class ServiceThread extends Thread {
		private Socket socket;
        private int clientNumber;

        public ServiceThread(Socket socket, int clientNumber) {
            this.socket = socket;
            this.clientNumber = clientNumber;
            System.out.println("New connection with client# " + clientNumber + " at " + socket);
        }

        /**
         * Services this thread's client by first sending the client a welcome message then repeatedly reading strings and 
         * sending back the results.
         */
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                // Get messages from the client, line by line; return the results
                int idx = -1;
                while (true) {
					String input = in.readLine();
					if (input == null) {
						break;
					}
					idx = input.indexOf(':');
					if (idx < 0) {
						System.out.println("Invalid input " + input + ", closing connection for client " + clientNumber);
						break;
					}
					String byWhat = input.substring(0, idx);
					if (byWhat.equalsIgnoreCase("userId")) {
						String uidTwin = input.substring(idx + 1);
						// uidTwin is like '12345' or '12345;[2012-02-04,2012-03-04]'
						int idxTwin = uidTwin.indexOf(";["); 
						if (idxTwin < 0) {
							String uid = uidTwin;
							getTweetsByUid(uid, out);
						} else {
							String twStr = uidTwin.substring(idxTwin + 2, uidTwin.length() - 1);
							int idxComma = twStr.indexOf(',');
							if (idxComma < 0) {
								out.println("Error: invalid input " + input);
							} else {
								String uid = uidTwin.substring(0, idxTwin);
								Map<String, long[]> oldTwInterval = twIntervals;
								twIntervals = TruthyHelpers.splitTimeWindowToMonths(twStr.substring(0, idxComma), twStr.substring(idxComma + 1));
								getTweetsByUid(uid, out);
								twIntervals = oldTwInterval;
							}
						}
					} else if (byWhat.equalsIgnoreCase("meme")) {
						String memeTwin = input.substring(idx + 1);
						// memeTwin is like '#tcot' or '#tcot;[2012-02-04,2012-03-04]'
						int idxTwin = memeTwin.indexOf(";["); 
						if (idxTwin < 0) {
							String meme = memeTwin;
							getTweetsByMeme(meme, out);
						} else {
							String twStr = memeTwin.substring(idxTwin + 2, memeTwin.length() - 1);
							int idxComma = twStr.indexOf(',');
							if (idxComma < 0) {
								out.println("Error: invalid input " + input);
							} else {
								String meme = memeTwin.substring(0, idxTwin);
								Map<String, long[]> oldTwInterval = twIntervals;
								twIntervals = TruthyHelpers.splitTimeWindowToMonths(twStr.substring(0, idxComma), twStr.substring(idxComma + 1));
								getTweetsByMeme(meme, out);
								twIntervals = oldTwInterval;
							}
						}
					} else if (byWhat.equalsIgnoreCase("sname")) {
						String snameTwin = input.substring(idx + 1);
						// snameTwin is like 'gaoxm' or 'gaoxm;[2012-02-04,2012-03-04]'
						int idxTwin = snameTwin.indexOf(";["); 
						if (idxTwin < 0) {
							String sname = snameTwin;
							getTweetsBySname(sname, out);
						} else {
							String twStr = snameTwin.substring(idxTwin + 2, snameTwin.length() - 1);
							int idxComma = twStr.indexOf(',');
							if (idxComma < 0) {
								out.println("Error: invalid input " + input);
							} else {
								String sname = snameTwin.substring(0, idxTwin);
								Map<String, long[]> oldTwInterval = twIntervals;
								twIntervals = TruthyHelpers.splitTimeWindowToMonths(twStr.substring(0, idxComma), twStr.substring(idxComma + 1));
								getTweetsBySname(sname, out);
								twIntervals = oldTwInterval;
							}
						}
					} else {
						System.out.println("Invalid input " + input + ", closing connection for client " + clientNumber);
						break;
					}
                }
                in.close();
                out.close();
            } catch (Exception e) {
                System.out.println("Error handling client# " + clientNumber + ": " + e);
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                	System.out.println("Couldn't close a socket, what's going on?");
                }
                System.out.println("Connection with client# " + clientNumber + " closed");
            }
        }
        
		/**
		 * Get tweets for the user identified by <b>uid</b>, and write the results to the socket output writer <b>pwSocket</b>.
		 * 
		 * @param uid
		 * @param pwSocket
		 * @throws Exception
		 */
		protected void getTweetsByUid(String uid, PrintWriter pwSocket) throws Exception {
			System.out.println("Getting tweets for user ID " + uid + "...");
			byte[] uidBytes = TruthyHelpers.getUserIdBytes(uid);
			getTweetsByUidBytes(uidBytes, pwSocket);
		}

		private void getTweetsByUidBytes(byte[] uidBytes, PrintWriter pwSocket) throws IOException, Exception {
			int count = nResPerReq;
			JsonArray jaRes = new JsonArray();
			for (String month : descMonths) {
				long[] times = twIntervals.get(month);
				if (times == null) {
					continue;
				}
				HTable userTweetsIdxTable = userTweetsIdxTableMap.get(month);
				TweetTableClient ttc = tweetGetterMap.get(month);
				Get utiGet = new Get(uidBytes);
				utiGet.setTimeRange(times[0], times[1] + 1);
				utiGet.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
				Result result = userTweetsIdxTable.get(utiGet);
				System.out.println("number of tweet IDs for " + month + ": " + result.size());
				if (result != null && result.size() > 0) {
					for (KeyValue cell : result.raw()) {
						byte[] tid = cell.getQualifier();
						String tweetId = "";
						if (ttc.isUseBigInt()) {
						    tweetId = TruthyHelpers.getTweetIDStrFromBigIntBytes(tid);
						} else {
						    tweetId = TruthyHelpers.getTweetIDStrFromBytes(tid);
						}						
						TweetData td = ttc.getTweetData(tweetId, true, true);
						jaRes.add(gson.toJsonTree(td, TweetData.class));
						count--;
						if (count <= 0) {
							break;
						}
					}
				}

				if (count <= 0) {
					break;
				}
			}
			pwSocket.println(jaRes.toString());
		}
		
		/**
		 * Get tweets for <b>meme</b>, and write the results to the socket output writer <b>pwSocket</b>.
		 * 
		 * @param meme
		 * @param pwSocket
		 * @throws Exception
		 */
		protected void getTweetsByMeme(String meme, PrintWriter pwSocket) throws Exception {
			System.out.println("Getting tweets for meme " + meme + "...");
			byte[] memeBytes = Bytes.toBytes(meme);
			JsonArray jaRes = new JsonArray();
			int count = nResPerReq;
			Scan scan = new Scan();
			scan.addFamily(ConstantsTruthy.CF_TWEETS_BYTES);
			scan.setBatch(nResPerReq);
			scan.setStartRow(memeBytes);
			scan.setStopRow(memeBytes);
			for (String month : descMonths) {
				long[] times = twIntervals.get(month);
				if (times == null) {
					continue;
				}
				scan.setTimeRange(times[0], times[1] + 1);
				HTable memeIdxTable = memeIdxTableMap.get(month);
				TweetTableClient ttc = tweetGetterMap.get(month);
				List<String> tids = new LinkedList<String>();
				ResultScanner rs = memeIdxTable.getScanner(scan);
				Result r = rs.next();
				while (r != null) {
					for (KeyValue cell : r.list()) {
						byte[] tid = cell.getQualifier();
						String tweetId = "";
						if (ttc.isUseBigInt()) {
						    tweetId = TruthyHelpers.getTweetIDStrFromBigIntBytes(tid);
						} else {
						    tweetId = TruthyHelpers.getTweetIDStrFromBytes(tid);
						}						
						tids.add(tweetId);
						count--;
						if (count <= 0) {
							break;
						}
					}
					if (count <= 0) {
						break;
					}
					r = rs.next();
				}
				rs.close();
				System.out.println("number of tweet IDs for " + month + ": " + tids.size());
				for (String tid : tids) {
					TweetData td = ttc.getTweetData(tid, true, true);
					jaRes.add(gson.toJsonTree(td, TweetData.class));
				}
				if (count <= 0) {
					break;
				}
			}			
			pwSocket.println(jaRes.toString());
		}
		
		/**
		 * Find the latest user ID matching <b>sname</b>, get tweets for the user, and write the results to the socket 
		 * output writer <b>pwSocket</b>.
		 * 
		 * @param uid
		 * @param pwSocket
		 * @throws Exception
		 */
		protected void getTweetsBySname(String sname, PrintWriter pwSocket) throws Exception {
			// first, find the latest user ID for sname			
			byte[] uidBytes = null;
			long latestTime = -1;
			for (String month : descMonths) {
				HTable snameIdxTable = snameIdxTableMap.get(month);
				Get siGet = new Get(Bytes.toBytes(sname.toLowerCase()));
				siGet.addFamily(ConstantsTruthy.CF_USERS_BYTES);
				Result result = snameIdxTable.get(siGet);
				if (result != null && result.size() > 0) {
					for (KeyValue cell : result.raw()) {
						if (cell.getTimestamp() > latestTime) {
							latestTime = cell.getTimestamp();
							uidBytes = cell.getQualifier();
						}
					}
					break;
				}
			}
			if (uidBytes == null) {
				System.out.println("Can't find user ID matching " + sname);
				pwSocket.write("[]");
				return;
			}
			System.out.println("Getting tweets for " + sname + "(" + TruthyHelpers.getUserIDStrFromBytes(uidBytes) + ")...");
			getTweetsByUidBytes(uidBytes, pwSocket);
		}
	}
	
	Configuration conf = null;
	Map<String, long[]> twIntervals = null;
	Map<String, TweetTableClient> tweetGetterMap = null;
	Map<String, HTable> userTweetsIdxTableMap = null;
	Map<String, HTable> snameIdxTableMap = null;
	Map<String, HTable> memeIdxTableMap = null;
	String[] descMonths = null;
	int port = -1;
	int nResPerReq = -1;
	Gson gson = null;
	
	public TruthyOnlineDemoServer(int port, String timeWinStart, String timeWinEnd, int nResPerReq) throws Exception {
		if (port < 0 || nResPerReq < 0) {
			throw new IllegalArgumentException("Port number or number of results per quest is set to <0.");
		}
		this.port = port;
		this.nResPerReq = nResPerReq;
		conf = HBaseConfiguration.create();
		twIntervals = TruthyHelpers.splitTimeWindowToMonths(timeWinStart, timeWinEnd);
		tweetGetterMap = new HashMap<String, TweetTableClient>();
		userTweetsIdxTableMap = new HashMap<String, HTable>();
		snameIdxTableMap = new HashMap<String, HTable>();
		memeIdxTableMap = new HashMap<String, HTable>();
		
		Stack<String> monthStack = new Stack<String>();
		for (String month : twIntervals.keySet()) {
			try {
				System.out.println("Creating HTables for " + month + "...");
				TweetTableClient tweetGetter = new TweetTableClient(ConstantsTruthy.TWEET_TABLE_NAME + "-" + month);
				HTable userTweetsIdxTable = new HTable(conf, ConstantsTruthy.USER_TWEETS_TABLE_NAME + "-" + month);
				HTable snameIdxTable = new HTable(conf, ConstantsTruthy.SNAME_INDEX_TABLE_NAME + "-" + month);
				HTable memeIdxTable = new HTable(conf, ConstantsTruthy.MEME_INDEX_TABLE_NAME + "-" + month);
				userTweetsIdxTableMap.put(month, userTweetsIdxTable);
				tweetGetterMap.put(month, tweetGetter);
				snameIdxTableMap.put(month, snameIdxTable);
				memeIdxTableMap.put(month, memeIdxTable);
				monthStack.push(month);
			} catch (Exception et) {
				System.out.println("Exception happend: " + et.getMessage());
				et.printStackTrace();
			}
		}
		
		descMonths = new String[monthStack.size()];
		for (int i=0; i<descMonths.length; i++) {
			descMonths[i] = monthStack.pop();
		}
		System.out.println("Months in descending order: " + Arrays.toString(descMonths));
		
		gson = new Gson();
	}
	
	/**
	 * Listening on a port, and create new service threads to handle clients.
	 * @throws Exception
	 */
	public void run() throws Exception {
		System.out.println("Start listening to port " + port + "...");
        int clientNumber = 0;
        ServerSocket listener = new ServerSocket(port);
        try {
            while (true) {
                new ServiceThread(listener.accept(), clientNumber++).start();
            }
        } finally {
            listener.close();
            for (TweetTableClient ttc : tweetGetterMap.values()) {
				ttc.close();
			}
			for (HTable rit : snameIdxTableMap.values()) {
				rit.close();
			}
			for (HTable mit : memeIdxTableMap.values()) {
				mit.close();
			}
			for (HTable uit: userTweetsIdxTableMap.values()) {
				uit.close();
			}
        }
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			usage();
			System.exit(1);
		}
		try {
			int port = Integer.valueOf(args[0]);
			String twStart = args[1];
			String twEnd = args[2];
			int nResPerReq = Integer.valueOf(args[3]);
			TruthyOnlineDemoServer demoServer = new TruthyOnlineDemoServer(port, twStart, twEnd, nResPerReq);
			demoServer.run();
		} catch (Exception e) {
			e.printStackTrace();
			usage();
			System.exit(1);
		}
	}
	
	public static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.TruthyOnlineDemoServer <port number to listen> <time window start>"
				+ " <time window end> <number of results per request>");
	}
}
