package opensource.hdata.plugin.reader.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import opensource.hdata.config.JobConfig;
import opensource.hdata.config.PluginConfig;
import opensource.hdata.core.plugin.Splitter;
import opensource.hdata.exception.HDataException;
import opensource.hdata.plugin.writer.hbase.HBaseWriterProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HBaseSplitter extends Splitter {

    private static final Logger LOG = LogManager.getLogger(HBaseSplitter.class);

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        int parallelism = readerConfig.getParallelism();

        String startRowkey = readerConfig.getString(HBaseReaderProperties.START_ROWKWY, "");
        String endRowkey = readerConfig.getString(HBaseReaderProperties.END_ROWKWY, "");
        byte[] startRowkeyBytes = startRowkey.getBytes();
        byte[] endRowkeyBytes = endRowkey.getBytes();

        if (parallelism == 1) {
            readerConfig.put(HBaseReaderProperties.START_ROWKWY, startRowkeyBytes);
            readerConfig.put(HBaseReaderProperties.END_ROWKWY, endRowkeyBytes);
            list.add(readerConfig);
            return list;
        } else {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_QUORUM));
            conf.set("hbase.zookeeper.property.clientPort", readerConfig.getString(HBaseReaderProperties.ZOOKEEPER_PROPERTY_CLIENTPORT, "2181"));
            try {
                HTable table = new HTable(conf, readerConfig.getString(HBaseWriterProperties.TABLE));
                Pair<byte[][], byte[][]> startEndKeysPair = table.getStartEndKeys();
                table.close();
                List<Pair<byte[], byte[]>> selectedPairList = new ArrayList<Pair<byte[], byte[]>>();
                byte[][] startKeys = startEndKeysPair.getFirst();
                byte[][] endKeys = startEndKeysPair.getSecond();

                if (startKeys.length == 1) {
                    Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                    pair.setFirst(startRowkeyBytes);
                    pair.setSecond(endRowkeyBytes);
                    selectedPairList.add(pair);
                } else {
                    if (startRowkeyBytes.length == 0 && endRowkeyBytes.length == 0) {
                        for (int i = 0, len = startKeys.length; i < len; i++) {
                            Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                            pair.setFirst(startKeys[i]);
                            pair.setSecond(endKeys[i]);
                            selectedPairList.add(pair);
                        }
                    } else if (endRowkeyBytes.length == 0) {
                        for (int i = 0, len = startKeys.length; i < len; i++) {
                            if (Bytes.compareTo(endKeys[i], startRowkeyBytes) >= 0) {
                                Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                                pair.setFirst(Bytes.compareTo(startKeys[i], startRowkeyBytes) >= 0 ? startKeys[i] : startRowkeyBytes);
                                pair.setSecond(endKeys[i]);
                                selectedPairList.add(pair);
                            }
                        }
                    } else {
                        for (int i = 0, len = startKeys.length; i < len; i++) {
                            if (len == 1) {
                                Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                                pair.setFirst(startRowkeyBytes);
                                pair.setSecond(endRowkeyBytes);
                                selectedPairList.add(pair);
                                break;
                            } else if (Bytes.compareTo(endKeys[i], startRowkeyBytes) >= 0 && Bytes.compareTo(endRowkeyBytes, startKeys[i]) >= 0) {
                                Pair<byte[], byte[]> pair = new Pair<byte[], byte[]>();
                                pair.setFirst(Bytes.compareTo(startKeys[i], startRowkeyBytes) >= 0 ? startKeys[i] : startRowkeyBytes);
                                pair.setSecond(Bytes.compareTo(endKeys[i], endRowkeyBytes) <= 0 ? endKeys[i] : endRowkeyBytes);
                                selectedPairList.add(pair);
                            }
                        }
                    }
                }

                if (parallelism > selectedPairList.size()) {
                    LOG.info(
                            "parallelism: {} is greater than the region count: {} in the currently open table: {}, so parallelism is set equal to region count.",
                            parallelism, selectedPairList.size(), Bytes.toString(table.getTableName()));
                    parallelism = selectedPairList.size();
                }

                double step = (double) selectedPairList.size() / parallelism;
                for (int i = 0; i < parallelism; i++) {
                    List<Pair<byte[], byte[]>> splitedPairs = new ArrayList<Pair<byte[], byte[]>>();
                    for (int start = (int) Math.ceil(step * i), end = (int) Math.ceil(step * (i + 1)); start < end; start++) {
                        splitedPairs.add(selectedPairList.get(start));
                    }
                    PluginConfig pluginConfig = (PluginConfig) readerConfig.clone();
                    pluginConfig.put(HBaseReaderProperties.START_ROWKWY, splitedPairs.get(0).getFirst());
                    pluginConfig.put(HBaseReaderProperties.END_ROWKWY, splitedPairs.get(splitedPairs.size() - 1).getSecond());
                    list.add(pluginConfig);
                }
            } catch (IOException e) {
                throw new HDataException(e);
            }

            return list;
        }
    }
}
