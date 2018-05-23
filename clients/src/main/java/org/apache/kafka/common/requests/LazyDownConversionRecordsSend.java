package org.apache.kafka.common.requests;

import org.apache.kafka.common.RecordsProcessingStats;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionRecordsStats;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.LazyDownConversionRecords;
import org.apache.kafka.common.record.LazyDownConversionRecordsIterator;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * Encapsulation for {@link RecordsSend} for {@link LazyDownConversionRecords}. Records are down-converted in batches and
 * on-demand when {@link #writeRecordsTo} method is called.
 */
public final class LazyDownConversionRecordsSend extends RecordsSend {
    private static final Logger log = LoggerFactory.getLogger(LazyDownConversionRecordsSend.class);

    private RecordsProcessingStats processingStats = null;
    private RecordsWriter convertedRecordsWriter = null;
    private final LazyDownConversionRecordsIterator convertedRecordsIterator;

    public LazyDownConversionRecordsSend(String destination, LazyDownConversionRecords records) {
        super(destination, records);
        this.convertedRecordsIterator = records.lazyDownConversionRecordsIterator();
    }

    @Override
    public long writeRecordsTo(GatheringByteChannel channel, long previouslyWritten, int remaining) throws IOException {
        if (convertedRecordsWriter == null || convertedRecordsWriter.remaining() == 0) {
            MemoryRecords convertedRecords;

            // Check if we have more chunks left to down-convert
            if (convertedRecordsIterator.hasNext()) {
                // Get next chunk of down-converted messages
                ConvertedRecords<MemoryRecords> recordsAndStats = convertedRecordsIterator.next();
                convertedRecords = recordsAndStats.records();

                if ((previouslyWritten == 0) && (convertedRecords.batchIterator().peek().sizeInBytes() > size()))
                    throw new EOFException("Unable to send first batch completely." +
                            " maximum_size: " + size() +
                            " converted_records_size: " + convertedRecords.batchIterator().peek().sizeInBytes());

                processingStats.addToProcessingStats(recordsAndStats.recordsProcessingStats());
                log.info("Got lazy converted records for {" + topicPartition() + "} with length=" + convertedRecords.sizeInBytes());
            } else {
                if (previouslyWritten == 0)
                    throw new EOFException("Unable to get the first batch of down-converted records");

                // We do not have any records left to down-convert. Construct a "fake" message for the length remaining.
                // This message will be ignored by the consumer because its length will be past the length of maximum
                // possible response size.
                // DefaultRecordBatch =>
                //      BaseOffset => Int64
                //      Length => Int32
                //      ...
                // TODO: check if there is a better way to encapsulate this logic, perhaps in DefaultRecordBatch
                log.info("Constructing fake message batch for topic-partition {" + topicPartition() + "} for remaining length " + remaining);
                int minLength = (Long.SIZE / Byte.SIZE) + (Integer.SIZE / Byte.SIZE);
                ByteBuffer fakeMessageBatch = ByteBuffer.allocate(Math.max(minLength, remaining + 1));
                fakeMessageBatch.putLong(-1L);
                fakeMessageBatch.putInt(remaining + 1);
                convertedRecords = MemoryRecords.readableRecords(fakeMessageBatch);
            }

            convertedRecordsWriter = new RecordsWriter(convertedRecords);
        }
        return convertedRecordsWriter.writeTo(channel, remaining);
    }

    @Override
    protected LazyDownConversionRecords records() {
        return (LazyDownConversionRecords) super.records();
    }

    public TopicPartitionRecordsStats recordsProcessingStats() {
        return new TopicPartitionRecordsStats(topicPartition(), processingStats);
    }

    private TopicPartition topicPartition() {
        return records().topicPartition();
    }

    /**
     * Implementation for writing {@link Records} to a particular channel. Internally tracks the progress of writes.
     */
    private static class RecordsWriter {
        private final Records records;
        private int position;

        RecordsWriter(Records records) {
            if (records == null)
                throw new IllegalArgumentException();
            this.records = records;
            position = 0;
        }

        private int position() {
            return position;
        }

        public int remaining() {
            return records.sizeInBytes() - position();
        }

        private void advancePosition(long numBytes) {
            position += numBytes;
        }

        public long writeTo(GatheringByteChannel channel, int length) throws IOException {
            int maxLength = Math.min(remaining(), length);
            long written = records.writeTo(channel, position(), maxLength);
            advancePosition(written);
            return written;
        }
    }
}
