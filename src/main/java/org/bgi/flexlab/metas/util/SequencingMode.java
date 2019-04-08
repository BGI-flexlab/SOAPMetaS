package org.bgi.flexlab.metas.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum SequencingMode {
    PAIREDEND("PE", "PAIREND", "PAIREDEND"),
    SINGLEEND("SE", "SINGLEEND");

    private static final Map<String, SequencingMode> modeMap = new HashMap<>(5);
    static {
        for (SequencingMode mode: SequencingMode.values()){
            for (String abbr: mode.name){
                modeMap.put(abbr, mode);
            }
        }
    }

    public static SequencingMode getValue(String modeName){
        assert modeMap.containsKey(modeName): "Wrong mode name of \"sequence\" argument.";
        return modeMap.get(modeName);
    }

    private final List<String> name;
    private SequencingMode(String ...args){
        this.name = Arrays.asList(args);
    }

}
