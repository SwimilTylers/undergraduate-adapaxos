package instance.store;

import instance.PaxosInstance;
import instance.StaticPaxosInstance;
import instance.store.index.OffsetIndexReader;
import instance.store.index.OffsetIndexWriter;

import java.io.*;

/**
 * @author : Swimiltylers
 * @version : 2019/2/19 14:39
 */
public class OffsetIndexStore implements InstanceStore{
    private OffsetIndexReader reader;
    private OffsetIndexWriter writer;

    private static final String ACCESS_ID_HEADER = "leader-";
    private static final String INST_ID_HEADER = "";

    public OffsetIndexStore(String store_name) {
        this.reader = new OffsetIndexReader("store"+File.separator+store_name);
        this.writer = new OffsetIndexWriter("store"+File.separator+store_name);
    }

    @Override
    synchronized public boolean isExist(int access_id, int inst_id) {
        if (reader.existIndex(ACCESS_ID_HEADER+access_id)){
            try {
                reader.locate(ACCESS_ID_HEADER+access_id);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }

            return reader.existInst(INST_ID_HEADER+inst_id);
        }
        else
            return false;
    }

    @Override
    synchronized public boolean store(int access_id, int inst_id, PaxosInstance instance) {
        if (writer.existIndex(ACCESS_ID_HEADER+access_id)){
            try {
                writer.relocate(ACCESS_ID_HEADER+access_id);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }
        }
        else {
            try {
                writer.create(ACCESS_ID_HEADER+access_id, true);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        ByteArrayOutputStream bstream = new ByteArrayOutputStream(1024);
        try {
            ObjectOutputStream ostream = new ObjectOutputStream(bstream);
            ostream.writeObject(instance);

            ostream.flush();
            ostream.close();

            byte[] inst = bstream.toByteArray();
            writer.update(INST_ID_HEADER+inst_id, inst);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    synchronized public PaxosInstance fetch(int access_id, int inst_id) {
        if (reader.existIndex(ACCESS_ID_HEADER+access_id)){
            try {
                reader.locate(ACCESS_ID_HEADER+access_id);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }

            byte[] raw = reader.readBytes(INST_ID_HEADER+inst_id);
            if (raw != null){
                try {
                    ObjectInputStream istream = new ObjectInputStream(new ByteArrayInputStream(raw));
                    return (PaxosInstance) istream.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                    return null;
                }
            }
            else
                return null;
        }
        else
            return null;
    }
}
