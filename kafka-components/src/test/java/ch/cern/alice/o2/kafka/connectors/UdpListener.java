package ch.cern.alice.o2.kafka.connectors;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class UdpListener extends Thread {

    private DatagramSocket socket;
    private boolean running;
    private byte[] buf = new byte[256];
    private int port;
    private List<String> messages = new ArrayList<String>();
    
    public UdpListener(int port) throws SocketException {
        this.port = port;
        socket = new DatagramSocket(this.port);
    }

    public void run() {
        running = true;
        while (running) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
            }
            String received = new String(packet.getData(), 0, packet.getLength());
            messages.add(received);
        }
        socket.close();
    }

    public List<String> stopAndReturn(){
        running = false;
        return this.messages;
    }
}