package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.inject.Provider;

public interface AnalyzerProvider<T extends Analyzer> extends Provider<T> {

    String name();

    AnalyzerScope scope();

    @Override
    T get();
}
