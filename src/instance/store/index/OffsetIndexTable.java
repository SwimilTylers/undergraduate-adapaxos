package instance.store.index;

import javafx.util.Pair;

import java.io.Serializable;
import java.util.Map;

/**
 * @author : Swimiltylers
 * @version : 2019/2/19 11:58
 */

class OffsetIndexTable implements Serializable {
    private static final long serialVersionUID = -8055045843812351988L;
    transient public static final String INDEX_FILE_NAME = ".index";

    String crtRollName;
    long crtRollOffset;

    Map<String, Pair<String, Pair<Long, Integer>>> queryTable;      // <inst_name, <storage_fileName, <offset, len>>>

    OffsetIndexTable(String crtRollName, long crtRollOffset, Map<String, Pair<String, Pair<Long, Integer>>> queryTable) {
        this.crtRollName = crtRollName;
        this.crtRollOffset = crtRollOffset;
        this.queryTable = queryTable;
    }
}
