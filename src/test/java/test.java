import java.io.IOException;
import java.net.Socket;

public class test {
    public static void main(String[] args) {
        String ip = "10.20.40.140";
        int port=22;

        try {
            Socket s = new Socket(ip, port);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }



}
