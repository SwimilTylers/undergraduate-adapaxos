package instance.store.index;

import javafx.util.Pair;

import java.io.*;
import java.util.HashMap;

import static instance.store.index.OffsetIndexTable.INDEX_FILE_NAME;

/**
 * @author : Swimiltylers
 * @version : 2019/2/19 12:02
 */
public class OffsetIndexWriter {
    private String storeBase = "store";
    private static final String DEFAULT_ROLL = "roll-1.store";

    private OffsetIndexTable crtOffsetTab = null;
    private File crtBase = null;

    @Override
    protected void finalize() throws Throwable {
        writeBack();
        super.finalize();
    }

    public boolean existIndex(String base_name){
        File test = new File(storeBase+File.separator+base_name+File.separator+INDEX_FILE_NAME);
        return test.exists();
    }

    private void write(String name, Object obj) throws IOException {
        FileOutputStream fstream = new FileOutputStream(name, false);
        ObjectOutputStream ostream = new ObjectOutputStream(fstream);
        ostream.writeObject(obj);
        ostream.flush();
        fstream.flush();

        ostream.close();
        fstream.close();
    }

    private void writeBack() throws IOException {
        if (crtOffsetTab != null && crtBase != null){
            write(crtBase+File.separator+INDEX_FILE_NAME, crtOffsetTab);
        }
    }

    public void create(String base_name, boolean checkout) throws IOException {
        File base = new File(storeBase+File.separator+base_name);
        if (!base.exists() || !base.isDirectory()){
            if (!base.mkdirs())
                return;
        }

        OffsetIndexTable tab = new OffsetIndexTable(DEFAULT_ROLL, 0, new HashMap<>());
        write(base+File.separator+INDEX_FILE_NAME, tab);

        if (checkout){
            crtOffsetTab = tab;
            crtBase = base;
        }
    }

    public void relocate(String base_name) throws IOException, ClassNotFoundException {
        writeBack();

        File base = new File(storeBase+File.separator+base_name+File.separator+INDEX_FILE_NAME);
        ObjectInputStream istream = new ObjectInputStream(new FileInputStream(base));
        crtOffsetTab = (OffsetIndexTable) istream.readObject();
        istream.close();

        crtBase = new File(storeBase+File.separator+base_name);
    }

    public void update(String inst_name, byte[] bytes) throws IOException {
        String fileName = crtOffsetTab.crtRollName;
        long offset = crtOffsetTab.crtRollOffset;
        int len = bytes.length;

        /* write to storage file first */

        RandomAccessFile file = new RandomAccessFile(crtBase+File.separator+fileName, "rws");
        file.seek(offset);
        file.write(bytes);

        file.close();

        /* update crtOffsetTab */

        crtOffsetTab.crtRollOffset += len;

        if (crtOffsetTab.queryTable.containsKey(inst_name))
            crtOffsetTab.queryTable.replace(inst_name, new Pair<>(fileName, new Pair<>(offset, len)));
        else
            crtOffsetTab.queryTable.put(inst_name, new Pair<>(fileName, new Pair<>(offset, len)));

        /* write back to the .index */

        writeBack();
    }
}
