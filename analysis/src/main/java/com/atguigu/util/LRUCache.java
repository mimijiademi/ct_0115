package com.atguigu.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 用于缓存已知的维度id，减少对mysql的操作次数，提高效率(父类直接拿过来的)
 * Created by MiYang on 2018/6/5 10:40.
 */
public class LRUCache extends LinkedHashMap<String,Integer> {
    private static final long serialVersionUID = 1L;
    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {

        return this.size() > this.maxElements;
    }
}
