package com.example;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

class Parse extends BaseFunction {
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = tuple.getString(0);
        String[] p = sentence.split(" ");
        if (p.length == 3 && "1".equals(p[2])) {
            collector.emit(new Values(Long.valueOf(p[1]), p[0]));
        }
    }
}
