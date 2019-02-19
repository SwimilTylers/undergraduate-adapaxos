package instance.store.index;

import java.io.*;

import static instance.store.index.OffsetIndexTable.INDEX_FILE_NAME;

/**
 * @author : Swimiltylers
 * @version : 2019/2/19 11:11
 */
public class OffsetIndexReader {
    private String rootPath;

    private OffsetIndexTable crtOffsetTab = null;
    private File crtBase = null;

    public OffsetIndexReader(String store_root) {
        rootPath = store_root;
    }

    public boolean existIndex(String base_name){
        File test = new File(rootPath +File.separator+base_name+File.separator+INDEX_FILE_NAME);
        return test.exists();
    }

    public boolean existInst(String inst_name){
        return crtOffsetTab.queryTable.containsKey(inst_name);
    }

    public void locate(String base_name) throws IOException, ClassNotFoundException {
        File base = new File(rootPath +File.separator+base_name+File.separator+INDEX_FILE_NAME);
        ObjectInputStream istream = new ObjectInputStream(new FileInputStream(base));
        crtOffsetTab = (OffsetIndexTable) istream.readObject();
        istream.close();

        crtBase = new File(rootPath +File.separator+base_name);
    }

    public byte[] readBytes(String inst_name){
        try {
            RandomAccessFile file = new RandomAccessFile(crtBase+File.separator+crtOffsetTab.queryTable.get(inst_name).getKey(), "r");
            long offset = crtOffsetTab.queryTable.get(inst_name).getValue().getKey();
            int len = crtOffsetTab.queryTable.get(inst_name).getValue().getValue();

            byte[] ret = new byte[len];
            file.seek(offset);
            file.readFully(ret);

            file.close();
            return ret;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
