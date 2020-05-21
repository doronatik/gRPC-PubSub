using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Pubsub;

namespace gRPCTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //Environment.SetEnvironmentVariable("GRPC_TRACE", "tcp,channel,http,secure_endpoint");
            //Environment.SetEnvironmentVariable("GRPC_VERBOSITY", "DEBUG");


            var channel = new Channel("127.0.0.1:50052", ChannelCredentials.Insecure/*, new[] { new ChannelOption("grpc.keepalive_time_ms", 10 * 1000),
                                                                                              new ChannelOption("grpc.keepalive_permit_without_calls", 1)}*/);
            var subscriber = new Subscriber(new PubSub.PubSubClient(channel));
            
            
            Task.Run(async () =>
            {
                while (true)
                {
                    Console.WriteLine($"Status: {channel.State}");
                    await Task.Delay(50);
                }
            });

            Task.Run(async () =>
            {
                while (true)
                {
                    await channel.WaitForStateChangedAsync(ChannelState.Ready);
                    Console.WriteLine("DISCONNECTION!!!!!!!!!!!!!!");
                }
            });


            channel.ConnectAsync();
            Subscribe(subscriber);

            Console.WriteLine("Hit 'q' to unsubscribe, 'c' to reconnect");
            char key;

            while ((key = Console.ReadKey().KeyChar) != 'q')
            {
                switch (key)
                {
                    case 's':
                        {
                            Console.WriteLine(channel.State);
                            break;
                        }
                    case 'c':
                        {
                            channel.ConnectAsync();
                            break;
                        }
                    case 'r':
                        {
                            Subscribe(subscriber);
                            break;
                        }
                    case 'q':
                        {
                            break;
                        }
                }
            }

            subscriber.Unsubscribe();

            Console.WriteLine("Unsubscribed...");

            Console.WriteLine("Hit key to exit...");
            Console.ReadLine();
            
        }

        private static void Subscribe(Subscriber subscriber)
        {
            Task.Run(async () =>
            {
                await subscriber.Subscribe();
            }).GetAwaiter();

        }

        public class Subscriber
        {
            private readonly PubSub.PubSubClient _pubSubClient;
            private Subscription _subscription;

            public Subscriber(PubSub.PubSubClient pubSubClient)
            {
                _pubSubClient = pubSubClient;
            } 

            public async Task Subscribe()
            {
                _subscription = new Subscription() { Id = Guid.NewGuid().ToString() };
                using (var call = _pubSubClient.Subscribe(_subscription))
                {
                    //Receive
                    var responseReaderTask = Task.Run(async () =>
                    {
                        while (await call.ResponseStream.MoveNext())
                        {
                            Console.WriteLine("Event received: " + call.ResponseStream.Current);
                        }
                    });

                    await responseReaderTask;
                }
            }

            public void Unsubscribe()
            {
                _pubSubClient.Unsubscribe(_subscription);
            }
        }
    }
}
