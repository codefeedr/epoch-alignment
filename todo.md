# TODO List

### Build a sinple generator for sample data
- Define and export Kibana configuration


### New generators
X Add a simple logging sink, which just logs latency and throughput statistics
- Deal with parallel generators (especially the fixed number of events per checkpoint type generator)
- Add deployment identification through the configurationprovidercomponent


### Intermediate measurements

- Implement backpressure in kafkaSink using the zookeeper state
- Align checkpoints, measure throughput




### Advanced Measurements
- Be able to perform a hot-swap