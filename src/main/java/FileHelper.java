import java.util.List;
import java.util.ArrayList;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class FileHelper {

    public FileHelper() {

    }

    public static List<String> readFileLines(String filePath, FileSystem fileSystem) {
        List<String> fileList = new ArrayList<>();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(fileSystem.open(new Path(filePath)));
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line = bufferedReader.readLine();
			while (line != null) {
				fileList.add(line);
				line = bufferedReader.readLine();
            }
            bufferedReader.close();
        } catch(IOException ioException) {
            System.out.println("Error reading " + filePath);
        }
        return fileList;
    }

    public static void writeToFile(String filePath, List<String> dataSet, int size, FileSystem fileSystem) {
        try {
            FSDataOutputStream outputStream = fileSystem.create(new Path(filePath));
            PrintWriter printWriter = new PrintWriter(outputStream);
            for(int i = 0; i < size; i++) {
                printWriter.write(dataSet.get(i) + "\n");
            }
            printWriter.close();
        } catch(IOException ioException) {
            System.out.println("Failed to write to file " + ioException.getMessage());
        }
    }

}