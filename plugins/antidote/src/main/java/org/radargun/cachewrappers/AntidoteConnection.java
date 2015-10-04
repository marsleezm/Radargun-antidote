package org.radargun.cachewrappers;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

import com.google.protobuf.MessageLite;

class AntidoteConnection {

	static final int DEFAULT_RIAK_PB_PORT = 8087;

	 private Socket sock;
	 private DataOutputStream dout;
	 private DataInputStream din;

	public AntidoteConnection(String host) throws IOException {
		this(host, DEFAULT_RIAK_PB_PORT);
	}

	public AntidoteConnection(String host, int port) throws IOException {
		this(InetAddress.getByName(host), port);
	}

	public AntidoteConnection(InetAddress addr, int port) throws IOException {
		System.out.println("Trying to connect...");
		sock = new Socket(addr, port);
		
		sock.setSendBufferSize(1024 * 200);
		
		dout = new DataOutputStream(new BufferedOutputStream(sock
				.getOutputStream(), 1024 * 200));
		din = new DataInputStream(
				new BufferedInputStream(sock.getInputStream(), 1024 * 200));
		System.out.println("Connected...");
	}

	///////////////////////

	
	void send(int code, MessageLite req) throws IOException {
		int len = req.getSerializedSize();
		dout.writeInt(len + 1);
		dout.write(code);
		req.writeTo(dout);
		dout.flush();
	}
	
	/*void send(MessageLite req) throws IOException {
		req.writeTo(dout);
		dout.flush();
	}
	void send(int code) throws IOException {
		dout.writeInt(1);
		dout.write(code);
		dout.flush();
	}*/

	
	byte[] receive(int code) throws IOException {
		int len = din.readInt();
		int get_code = din.read();


		byte[] data = null;
		if (len > 1) {
			data = new byte[len - 1];
			din.readFully(data);
		}

		if (code != get_code) {
			throw new IOException("bad message code");
		}

		return data;
	}
	
	byte[] receive() throws IOException {
		int len = din.readInt();

		/*if (code == RiakClient.MSG_ErrorResp) {
			RpbErrorResp err = com.trifork.riak.RPB.RpbErrorResp.parseFrom(din);
			throw new IOException(err.getErrmsg().toStringUtf8());
		}*/

		byte[] data = null;
		if (len > 1) {
			data = new byte[len];
			din.readFully(data);
		}


		return data;
	}
	

	void receive_code(int code) throws IOException {
		int len = din.readInt();
		int get_code = din.read();
		/*if (code == RiakClient.MSG_ErrorResp) {
			RpbErrorResp err = com.trifork.riak.RPB.RpbErrorResp.parseFrom(din);
			throw new IOException(err.getErrmsg().toStringUtf8());
		}*/
		if (len != 1 || code != get_code) {
			throw new IOException("bad message code");
		}
	}

	static Timer timer = new Timer();
	TimerTask idle_timeout;
	
	public void beginIdle() {
		idle_timeout = new TimerTask() {
			
			@Override
			public void run() {
				AntidoteConnection.this.timer_fired(this);
			}
		};
		
		timer.schedule(idle_timeout, 1000);
	}

	synchronized void timer_fired(TimerTask fired_timer) {
		if (idle_timeout != fired_timer) {
			// if it is not our current timer, then ignore
			return;
		}
		
		close();
	}

	void close() {
		if (isClosed())
			return;
		
		try {
			sock.close();
			din = null;
			dout = null;
			sock = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	synchronized boolean endIdleAndCheckValid() {
		TimerTask tt = idle_timeout;
		if (tt != null) { tt.cancel(); }
		idle_timeout = null;
		
		if (isClosed()) {
			return false;
		} else {
			return true;
		}
	}

	public DataOutputStream getOutputStream() {
		return dout;
	}

	public boolean isClosed() {
		return sock == null || sock.isClosed();
	}
	
	
}
