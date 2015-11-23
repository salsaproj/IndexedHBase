package iu.pti.hbaseapp.truthy;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TruthyOnlineDemoClient {
	protected BufferedReader socketIn;
	protected PrintWriter socketOut;
	String serverIP = null;
	int serverPort = -1;
	
	public TruthyOnlineDemoClient(String serverIP, int serverPort) {
		this.serverIP = serverIP;
		this.serverPort = serverPort;
	}
	
	/**
	 * Connect to the server, and then keep sending user's input to the server and print the results
	 * @throws Exception
	 */
	public void run() throws Exception {
		System.out.print("Contacting to server...");
		Socket socket = new Socket(serverIP, serverPort);
		BufferedReader socketIn = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		PrintWriter socketOut = new PrintWriter(socket.getOutputStream(), true);
		BufferedReader brStdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("connected.");
		long startTime = -1;
		long endTime = -1;
		while (true) {
			try {
				System.out.println("Please input query (type 'exit' to exit the program): ");
				String input = brStdin.readLine();
				if (input.equalsIgnoreCase("exit")) {
					break;
				}
				socketOut.println(input);
				startTime = System.currentTimeMillis();
				String answer = socketIn.readLine();
				endTime = System.currentTimeMillis();
				if (answer == null) {
					System.out.println("Null answer from the server. Quiting...");
					break;
				}				
				System.out.println("Server answer: ");
				System.out.println(answer);
				System.out.println("Time for getting answer (ms): " + (endTime - startTime));
			} catch (Exception e) {
				System.out.println("Error when talking to server: " + e.getMessage());
				break;
			}
		}
		socketIn.close();
		socketOut.close();
		brStdin.close();
		socket.close();
	}

	/**
	 * Runs the client application.
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			usage();
		}
		try {
			String serverIP = args[0];
			int serverPort = Integer.valueOf(args[1]);
			TruthyOnlineDemoClient client = new TruthyOnlineDemoClient(serverIP, serverPort);
			client.run();
		} catch (Exception e) {
			e.printStackTrace();
			usage();
			System.exit(1);
		}
	}
	
	public static void usage() {
		System.out.println("Usage: java iu.pti.hbaseapp.truthy.TruthyOnlineDemoClient <server IP> <server port>");
	}
}
