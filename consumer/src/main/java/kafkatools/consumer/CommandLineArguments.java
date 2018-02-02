package kafkatools.consumer;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class CommandLineArguments {
  @Option(names = { "-t", "--topic" }, required = true, description = "The topic to consume")
  public String topic = "";

  @Option(names = { "-g", "--group" }, required = true, description = "The consumer group id")
  public String groupId = "";

  @Option(names = { "-b", "--brokers" }, required = true, description = "The list of brokers")
  public String brokers = "";

  @Option(names = { "-p", "--partition-assignment" }, required = true, description = "The list of brokers")
  public String partitionAssignment = "roundrobin";

}

