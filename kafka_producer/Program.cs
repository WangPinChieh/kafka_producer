using System;
using Confluent.Kafka;

namespace kafka_producer
{
	class Program
	{
		static void Main(string[] args)
		{
			var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092"};
			Action<DeliveryReport<Null, string>> handler = r => Console.WriteLine(
				!r.Error.IsError ? $"Delivered message to {r.TopicPartitionOffsetError}" : $"Delivery error: {r.Error.Reason}");

			using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
			{
				for (var i = 0; i < 100; i++)
				{
					var produceAsync = producer.ProduceAsync("MyVeryFirstTopic", new Message<Null, string>
					{
						Value = $"Producer message: {i}"
					});
					var deliveryResult = produceAsync.GetAwaiter().GetResult();
					Console.WriteLine($"Producer message: {i}");
				}
				producer.Flush(TimeSpan.FromSeconds(10));
			}

			Console.ReadLine();
		}
	}
}
