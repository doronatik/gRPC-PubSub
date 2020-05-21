using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Grpc.Core;
using Pubsub;

namespace gRPCTest.Server
{
    class Program
    {
        static void Main(string[] args)
        {
            Environment.SetEnvironmentVariable("GRPC_TRACE", "tcp,channel,http,secure_endpoint");
            Environment.SetEnvironmentVariable("GRPC_VERBOSITY", "DEBUG");

            //List<ChannelOption> channelOptions = new List<ChannelOption>();
            //channelOptions.Add(new ChannelOption("grpc.http2.min_ping_interval_without_data_ms", 1000));
            //channelOptions.Add(new ChannelOption("grpc.http2.max_pings_without_data", 0));
            //channelOptions.Add(new ChannelOption("grpc.keepalive_permit_without_calls", 1));
            //channelOptions.Add(new ChannelOption("grpc.keepalive_time_ms", 10 * 1000));

            const int port = 50052;
            var pubsubImp = new PubSubImpl();
            Grpc.Core.Server server = new Grpc.Core.Server(/*channelOptions*/)
            {
                Services = { PubSub.BindService(pubsubImp) },
                Ports = { new ServerPort("localhost", port, ServerCredentials.Insecure)}
            };

            server.Start();

            Console.WriteLine("RouteGuide server listening on port " + port);
            Console.WriteLine("Insert event. 'q' to quit.");
            string input;
            while ((input = Console.ReadLine()) != "q")
            {
                pubsubImp.Publish(input);
            }

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }

    public class PubSubImpl : PubSub.PubSubBase
    {
        private readonly BufferBlock<Event> _buffer = new BufferBlock<Event>();

        private Dictionary<string, IServerStreamWriter<Event>> _subscriberWritersMap = new Dictionary<string, IServerStreamWriter<Event>>();

        public override async Task Subscribe(Subscription subscription, IServerStreamWriter<Event> responseStream, ServerCallContext context)
        {
            _subscriberWritersMap[subscription.Id] = responseStream;

            while (_subscriberWritersMap.ContainsKey(subscription.Id))
            {
                var ev = await _buffer.ReceiveAsync();
                foreach (var serverStreamWriter in _subscriberWritersMap.Values)
                {
                    try
                    {
                        await serverStreamWriter.WriteAsync(ev);
                    }
                    catch (Exception e)
                    {
                        //We should remove this dictionary entry, but it causes issues...
                        //_subscriberWritersMap.Remove(subscription.Id);
                    }
                }
            }
        }

        public override Task<Unsubscription> Unsubscribe(Subscription request, ServerCallContext context = null)
        {
            _subscriberWritersMap.Remove(request.Id);
            return Task.FromResult(new Unsubscription() { Id = request.Id });
        }

        public void Publish(string input)
        {
            //Only publish when there are subscribers
            if (_subscriberWritersMap.Count > 0)
            {
                _buffer.Post(new Event() { Value = input });
            }
        }
    }
}
