// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using LanguageExt;
using surge.multilanguage.protobuf;

namespace Surge
{
    public class SurgeEngine<TS, TE, TC>
    {
        private readonly MultilanguageGatewayService.MultilanguageGatewayServiceClient _client;

        // ReSharper disable once IdentifierTypo
        private readonly SerDeser<TS, TE, TC> _serDeser;
        private readonly CqrsModel<TS, TE, TC> _cqrsModel;

        // ReSharper disable once IdentifierTypo
        public SurgeEngine(SerDeser<TS, TE, TC> serDeser, CqrsModel<TS, TE, TC> cqrsModel)
        {
            this._serDeser = serDeser;
            this._cqrsModel = cqrsModel;

            string localServerHost = Environment.GetEnvironmentVariable("LOCAL_SERVER_HOST") ?? "127.0.0.1";
            int localServerPort = int.Parse(Environment.GetEnvironmentVariable("LOCAL_SERVER_PORT") ?? "7777");
            string surgeServerHost = Environment.GetEnvironmentVariable("SURGE_SERVER_HOST") ?? "127.0.0.1";
            int surgeServerPort = int.Parse((Environment.GetEnvironmentVariable("SURGE_SERVER_PORT") ?? "6667"));

            Console.WriteLine($"Surge side car accessible via gRPC: {surgeServerHost}:{surgeServerPort}!");
            Console.WriteLine(
                $"Local gRPC business logic server going to be bound on: {localServerHost}:{localServerPort}!");

            var grpcChannel = new Channel(surgeServerHost, surgeServerPort, ChannelCredentials.Insecure);

            _client = new MultilanguageGatewayService.MultilanguageGatewayServiceClient(grpcChannel);

            Console.WriteLine("Starting gRPC business logic server!");
            var server = new Server
            {
                Services =
                {
                    BusinessLogicService.BindService(new BusinessLogicServiceImpl<TS, TE, TC>(cqrsModel, serDeser))
                },
                Ports =
                {
                    new ServerPort(localServerHost, localServerPort, ServerCredentials.Insecure)
                }
            };
            server.Start();

        }

        public Task<Option<TS>> GetState(Guid aggregateId)
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

        public Task<Option<TS>> ForwardCommand(Guid aggregateId, TC cmd)
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
                    // ReSharper disable once SuggestVarOrType_Elsewhere
                    Task<Option<TS>> failedTask =
                        Task<Option<TS>>.Factory.StartNew(() => throw new Exception(reply.RejectionMessage));
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
                        var s = _serDeser.DeserializeState(reply.NewState.Payload.ToByteArray());
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
