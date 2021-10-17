using System;
using Grpc.Core;
using Grpc.Net.Client;
using surge.multilanguage.protobuf;
using System.Threading.Tasks;
using Google.Protobuf;

using LanguageExt;

namespace Surge 
{
    public class BridgeToSurge<TS, TE, TC>
    {
        private readonly MultilanguageGatewayService.MultilanguageGatewayServiceClient _client;
        private readonly SerDeser<TS, TE, TC> _serDeser; 

        public BridgeToSurge(SerDeser<TS, TE, TC> serDeser)
        {
            this._serDeser= serDeser;
            string surgeServerHost = Environment.GetEnvironmentVariable("SURGE_SERVER_HOST") ?? "127.0.0.1";
            int surgeServerPort = Int32.Parse((Environment.GetEnvironmentVariable("SURGE_SERVER_PORT") ?? "6667"));

            var uriBuilder = new UriBuilder
            {
                Host = surgeServerHost,
                Port = surgeServerPort,
                Scheme = "http"
            };
            var uri = uriBuilder.Uri;

            var grpcChannelOptions = new GrpcChannelOptions
            {
                Credentials = ChannelCredentials.Insecure
            };

            var grpcChannel = GrpcChannel.ForAddress(uri, grpcChannelOptions);

            _client = new MultilanguageGatewayService.MultilanguageGatewayServiceClient(grpcChannel);
            
        }

        private Task<Option<TS>> GetState(Guid aggregateId)
        {
            var getStateRequest = new GetStateRequest
            {
                AggregateId = null
            };
            getStateRequest.AggregateId = aggregateId.ToString();
            var result = _client.GetStateAsync(getStateRequest);

            Option<TS> ParseReply(GetStateReply reply)
            {
                if (reply.State.IsNull())
                {
                    return Option<TS>.None;
                }
                else
                {
                    var deserializedState = _serDeser.DeserializeState(reply.State.Payload.ToByteArray());
                    return Option<TS>.Some(deserializedState);
                }
            }

            return result.ResponseAsync.Map((Func<GetStateReply, Option<TS>>) ParseReply);
        }

        private Task<Option<TS>> ForwardCommand(Guid aggregateId, TC cmd)
        {
            var command = new Command
            {
                AggregateId = aggregateId.ToString(),
                Payload = ByteString.CopyFrom(_serDeser.SerializeCommand(cmd))
            };
            var forwardCommandRequest = new ForwardCommandRequest
            {
                AggregateId = aggregateId.ToString(),
                Command = command
            };

            Task<Option<TS>> ParseReply(ForwardCommandReply reply)
            {
                Option<TS> result;
                if (!reply.IsSuccess)
                {
                    Task<Option<TS>> failedTask = Task<Option<TS>>.Factory.StartNew(() => throw new Exception(reply.RejectionMessage));
                    return failedTask;
                }
                else
                {
                    if (reply.NewState.IsNull())
                    {
                      result = Option<TS>.None;
                    }
                    else
                    {
                        var s = _serDeser.DeserializeState(reply.NewState.ToByteArray());
                        result = Option<TS>.Some(s);
                    }
                    return Task.Factory.StartNew(() => result);
                } 
            }
            
            var result = _client.ForwardCommandAsync(forwardCommandRequest);

            return result.ResponseAsync.MapAsync(ParseReply);
        }
        
        
    }
}