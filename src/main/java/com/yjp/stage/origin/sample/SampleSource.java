/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yjp.stage.origin.sample;

import com.alibaba.fastjson.JSONObject;
import com.streamsets.pipeline.api.*;
import com.yjp.stage.lib.sample.Errors;
import com.streamsets.pipeline.api.base.BaseSource;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 功能介绍，该插件是使用kudu的API将从kudu中获取数据。
 * 可以按照指定的条件进行获取，例如timestamp、long、int类型进行获取kudu的数据。
 */
public abstract class SampleSource extends BaseSource {
    private static final Logger LOG = LoggerFactory.getLogger(SampleSource.class);

    /**
     * 所需的配置有:kudu master 的连接信息，所需要抽取的表，条件查询的区间1，条件查询的区间2，条件类型
     */
    public abstract String getConfig();

    private static final String kuduMaster = "master4.cloudera.yijiupidev.com,master2.cloudera.yijiupidev.com,master3.cloudera.yijiupidev.com";
    private static final String tableName = "impala::test.student";
    private static final long queryConditionSta = 20200912;
    private static final long queryConditionEnd = 0;
    private static final String queryConditionType = "long";
    private static final String checkColumn = "date_key";
    private static KuduClient client;
    private static KuduTable kuduTable;
    private static KuduScanner.KuduScannerBuilder builder;
    private static List<ColumnSchema> fieldList;
    private static KuduScanner scanner;

    protected KuduScanner setPredicate(KuduScanner.KuduScannerBuilder builder, KuduTable table, String checkColumn, long offset) {
        KuduPredicate predicate1 = KuduPredicate.newComparisonPredicate(table.getSchema().getColumn(checkColumn),
                KuduPredicate.ComparisonOp.GREATER, offset);
        builder.addPredicate(predicate1);

        return builder.build();
    }

    /**
     * SDC在验证和运行管道时调用init()方法。示例展示了如何报告配置错误。
     *
     * @return
     */
    @Override
    protected List<ConfigIssue> init() {
        // 初始化kudu连接信息
        List<ConfigIssue> issues = super.init();
        try {

            client = new KuduClient.KuduClientBuilder(kuduMaster).defaultSocketReadTimeoutMs(6000).build();

            kuduTable = client.openTable(tableName);

            builder = client.newScannerBuilder(kuduTable);

            fieldList = kuduTable.getSchema().getColumns();

            scanner = setPredicate(builder, kuduTable, checkColumn, queryConditionSta);
            LOG.info("connected to kudu  at {}", client.getLocationString());
        } catch (Exception e) {
            LOG.error("Exception building get kudu client {}", e.getMessage());
            e.printStackTrace();
            issues.add(getContext().createConfigIssue(Groups.SAMPLE.name(), "kuduConfig", StageUpgrader.Error.UPGRADER_00, e.getLocalizedMessage()));
        }
        return issues;
    }

    /**
     * {@inheritDoc}
     * 释放init中的资源，
     * 1.清除kudu的connect
     */
    @Override
    public void destroy() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOG.error("close kudu client failed");
        }
    }

    /**
     * {@inheritDoc}
     * SDC将反复调用该命令以创建成批的记录
     */
    // {"committer":{"name":"Pat Patterson","email":"user@example.com"},"short_message":"commit 1","time":1481155617,"hash":"63e87c1e97f0ccfb91a644c186291ffa78102998"}
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
        int numRecords = 0;
        if (!scanner.hasMoreRows()) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                LOG.error("Sleep interrupt", e);
            }
        } else {
            try {
                while (scanner.hasMoreRows() && numRecords < maxBatchSize) {
                    RowResultIterator results = scanner.nextRows();
                    int numRows = results.getNumRows();
                    LOG.info("numRows count is {}", numRows);
                    while (results.hasNext()) {
                        Map<String, Field> map = new HashMap<>();
                        map.put("tablename", Field.create(tableName));
                        RowResult result = results.next();
                        JSONObject json = new JSONObject();
                        for (ColumnSchema schema : fieldList) {
                            Object obj = KuduUtil.getObject(result, schema.getType(), schema.getName(), "mysql");
                            json.put(schema.getName(), obj);
                            map.put(schema.getName(), Field.create(1));
                        }
                        Record record = getContext().createRecord(tableName);
                        record.set(Field.create(map));
                        batchMaker.addRecord(record);
                    }
                    numRecords++;
                }
            } catch (Exception e) {
                LOG.info(e.getLocalizedMessage());
            }
        }


        return null;
    }

}
