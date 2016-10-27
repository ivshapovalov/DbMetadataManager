package com.juja.pairs.model;

import java.io.*;

/**
 * Created by ВаНо on 27.10.2016.
 */
public class FileContentGetter {

    /* before fetching this method make sure that
     * the appropriate file with necessary input parameters
     * was located in the resources package.
    */
    public static String getTestContent(String fileName){
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
    public static String getContent(String fileName){
        String readLine;
        StringBuilder builder = new StringBuilder();

        try (BufferedReader in = new BufferedReader(new FileReader(fileName))){
            while ((readLine = in.readLine()) != null){
                builder.append(readLine).append("\n");
            }
        } catch (FileNotFoundException fe){
            return getTestContent(fileName);
        } catch (IOException io){
            io.printStackTrace();
        }




        return builder.toString();

    }
}
