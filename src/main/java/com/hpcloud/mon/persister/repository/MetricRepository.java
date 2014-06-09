package com.hpcloud.mon.persister.repository;

public interface MetricRepository {
    void addMetricToBatch(Sha1HashId defDimsId, String timeStamp, double value);

    void addDefinitionToBatch(Sha1HashId defId, String name, String tenantId, String region);

    void addDimensionToBatch(Sha1HashId dimSetId, String name, String value);

    void addDefinitionDimensionToBatch(Sha1HashId defDimsId, Sha1HashId defId, Sha1HashId dimId);

    void flush();
}
