package com.juja.pairs.view;

import org.apache.log4j.Logger;

import java.io.*;

import static com.juja.pairs.DbMetadataManager.*;

public class FileView implements View {
    private String filePath;
    private final static Logger logger = Logger.getLogger(FileView.class);
    static {
        logger.addAppender(logAppender);
    }

    public FileView(String filePath) {
        this.filePath = filePath;

    }

    @Override
    public void write(String message) {
        //TODO write to file
        System.out.println(message);
    }

    @Override
    public String read() {
        String readLine;
        StringBuilder builder = new StringBuilder();

        try (BufferedReader in = new BufferedReader(new FileReader(filePath))){
            while ((readLine = in.readLine()) != null){
                builder.append(readLine).append("\n");
            }
        } catch (FileNotFoundException fe){
            return getStubContent();//TODO replace when finished the project
        } catch (IOException io){
            io.printStackTrace();
        }
        return builder.toString();
    }

    /* before fetching this method make sure that
    * the appropriate file with necessary input parameters
    * was located in the resources package.
   */
    private static String getStubContent(){
        String fileName = "PostgresConParam.txt";
        String readLine;
        StringBuilder builder = new StringBuilder();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(fileName);

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
            while ((readLine = in.readLine()) != null){
                builder.append(readLine).append("\n");
            }
        }catch (FileNotFoundException fe){
            fe.printStackTrace();
        }catch (IOException io){
            io.printStackTrace();
        }
        return builder.toString();
    }
}
