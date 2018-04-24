from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test

from kafkatest.services.kafka import KafkaService
from kafkatest.services.performance import ProducerPerformanceService, ConsumerPerformanceService, \
    compute_aggregate_throughput
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import DEV_BRANCH, LATEST_0_10, KafkaVersion


class DownconversionMemoryTest(Test):

    def __init__(self, test_context):
        super(DownconversionMemoryTest, self).__init__(test_context=test_context)
        # Producer and consumer
        self.max_messages = 1000000
        self.producer_throughput = self.max_messages
        self.timeout_sec = 2*60
        self.num_producers = 1
        self.num_consumers = 7
        self.message_size = 1024
        self.batch_size = self.message_size * 50
        self.fetch_size = 12 * 1024 * self.message_size
        self.num_partitions = 12
        self.topics = ["test_topic"]
        self.zk = ZookeeperService(self.test_context, num_nodes=1)

    def setUp(self):
        self.zk.start()

    @cluster(num_nodes=12)
    @parametrize(producer_version=str(DEV_BRANCH), consumer_version=str(LATEST_0_10))
    def test_downconversion(self, producer_version, consumer_version):
        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk, version=DEV_BRANCH,
                                  topics={topic:
                                              {"partitions": self.num_partitions,
                                               "replication-factor": 1,
                                               "configs": {"min.insync.replicas": 1}}
                                          for topic in self.topics},
                                  heap_opts="-Xmx256M -Xms256M",
                                  jmx_object_names=['java.lang:type=Memory'],
                                  jmx_attributes=['HeapMemoryUsage'],
                                  jmx_attribute_keys=['used'],
                                  jmx_manual_start=True)
        self.kafka.start()

        # seed kafka with messages
        for topic in self.topics:
            producer = ProducerPerformanceService(
                self.test_context, 1, self.kafka,
                topic=topic,
                num_records=self.max_messages, record_size=self.message_size, throughput=-1, version=producer_version,
                settings={
                    'acks': 1,
                    'batch.size': self.batch_size
                }
            )
            producer.run()
            print("Producer throughput:")
            print(compute_aggregate_throughput(producer))

        # start monitoring JMX
        for node in self.kafka.nodes:
            self.kafka.start_jmx_tool(self.kafka.idx(node), node)

        # consume
        for topic in self.topics:
            consumer = ConsumerPerformanceService(
                self.test_context, self.num_consumers, self.kafka,
                topic=topic, messages=self.max_messages, version=KafkaVersion(consumer_version), new_consumer=True)
            #consumer.group = "test-consumer-group"
            consumer.run()

        self.kafka.read_jmx_output_all_nodes()

        heap_memory_usage_mbean = 'java.lang:type=Memory:HeapMemoryUsage'
        print("Average heap usage: %.2f" % self.kafka.average_jmx_value[heap_memory_usage_mbean])
        print("Maximum heap usage: %.2f" % self.kafka.maximum_jmx_value[heap_memory_usage_mbean])

        return compute_aggregate_throughput(consumer)

