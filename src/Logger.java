package pgps;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.text.*;
import java.io.*;

public class Logger {

    private BufferedOutputStream stream;
    
    public Logger(String filename) throws FileNotFoundException {
        this.stream = new BufferedOutputStream(new FileOutputStream(filename));
    }
    
    public Logger(File f) throws FileNotFoundException {
        this.stream = new BufferedOutputStream(new FileOutputStream(f));
    }
    
    public void log(String s) throws IOException {
        SimpleDateFormat ft = new SimpleDateFormat ("hh:mm:ss:SSS");
        //String date = new Date().toString();
        String message = String.format("%s : %s", ft.format(new Date()), s);
        
        this.stream.write(message.getBytes());
        this.stream.write(System.lineSeparator().getBytes());
        this.stream.flush();
    }
    
    public void close() throws IOException {
        this.stream.close();
    }
}