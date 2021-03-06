// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using LanguageExt;
using surge.multilanguage.protobuf;

namespace Surge
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class BusinessLogicServiceImpl<TS, TE, TC> : BusinessLogicService.BusinessLogicServiceBase
    {
        public BusinessLogicServiceImpl(CqrsModel<TS, TE, TC> cqrsModel, SerDeser<TS, TE, TC> serDeser)
        {
            CqrsModel = cqrsModel;
            SerDeser = serDeser;
        }

        private CqrsModel<TS, TE, TC> CqrsModel { get; }

        private SerDeser<TS, TE, TC> SerDeser { get; }

        public override Task<HealthCheckReply> HealthCheck(HealthCheckRequest request, ServerCallContext context)
        {
            var reply = new HealthCheckReply
            {
                Status = HealthCheckReply.Types.Status.Up,
                ServiceName = "BusinessLogicService"
            };
            return Task<HealthCheckReply>.Factory.StartNew(() => reply);
        }

        private ProcessCommandReply DoProcessCommand(ProcessCommandRequest request)
        {
            Option<TS> maybeState = Option<TS>.None;
            if (!request.State.IsNull())
            {
                // deserialize state
                var state = SerDeser.DeserializeState.Invoke(request.State.Payload.ToByteArray());
                maybeState = new Some<TS>(state);
            }

            // deserialize command
            // TODO: log some warning if command is not present (should never happen)
            // ReSharper disable once SuggestVarOrType_SimpleTypes
            TC command = SerDeser.DeserializeCommand.Invoke(request.Command.Payload.ToByteArray());

            // ReSharper disable once SuggestVarOrType_Elsewhere
            Either<string, List<TE>> result = CqrsModel.CommandHandler.Invoke(Tuple.Create(maybeState, command));

            switch (result.IsLeft)
            {
                case true:
                {
                    var reply = new ProcessCommandReply
                    {
                        AggregateId = request.AggregateId,
                        IsSuccess = false,
                        RejectionMessage = result.LeftToSeq().Head
                    };
                    return reply;
                }
                default:
                {
                    List<TE> events = result.RightToSeq().Head;
                    Option<TS> zeroState = Option<TS>.None;
                    // calculate the new state by doing a left over the events 
                    Option<TS> newState = events.Fold(zeroState, (ses, e) =>
                        CqrsModel.EventHandler.Invoke(Tuple.Create(ses, e)));

                    IEnumerable<Event> eventsPb = events.Map(item => new Event()
                    {
                        AggregateId = request.AggregateId,
                        Payload = ByteString.CopyFrom(SerDeser.SerializeEvent.Invoke(item))
                    });

                    State newStatePb = newState.IsSome switch
                    {
                        true => new State
                        {
                            AggregateId = request.AggregateId,
                            // Serialize the calculated state to protocol buffers
                            Payload = ByteString.CopyFrom(SerDeser.SerializeState.Invoke(newState.ToList().Head()))
                        },
                        _ => null
                    };

                    ProcessCommandReply reply;
                    reply = new ProcessCommandReply
                    {
                        NewState = newStatePb,
                        AggregateId = request.AggregateId,
                        IsSuccess = true,
                        RejectionMessage = string.Empty,
                    };
                    foreach (var @event in eventsPb)
                    {
                        reply.Events.Add(@event);
                    }


                    return reply;
                }
            }
        }

        private HandleEventsResponse DoHandleEvents(HandleEventsRequest handleEventsRequest)
        {
            Option<TS> state = Option<TS>.None;
            if (!handleEventsRequest.State.IsNull())
            {
                // deserialize the state (from protocol buffers) if it's there
                state = Option<TS>.Some(
                    SerDeser.DeserializeState.Invoke(handleEventsRequest.State.Payload.ToByteArray()));
            }

            // deserialize all events (from protocol buffers)
            // ReSharper disable once SuggestVarOrType_Elsewhere
            IEnumerable<TE> events =
                handleEventsRequest.Events.Map(e => SerDeser.DeserializeEvent(e.Payload.ToByteArray()));
            // calculate new state
            Option<TS> newState = events.Fold(state, (ses, e) => CqrsModel.EventHandler.Invoke(Tuple.Create(ses, e)));

            // serialize new state to protocol buffers
            State resultingStatePb = newState.IsSome switch
            {
                true => new State
                {
                    AggregateId = handleEventsRequest.AggregateId,
                    Payload = ByteString.CopyFrom(SerDeser.SerializeState.Invoke(newState.ToList().Head()))
                },
                _ => null
            };

            var result = new HandleEventsResponse
            {
                AggregateId = handleEventsRequest.AggregateId,
                State = resultingStatePb
            };
            return result;
        }

        public override Task<ProcessCommandReply> ProcessCommand(ProcessCommandRequest request,
            ServerCallContext context)
        {
            return Task<ProcessCommandReply>.Factory.StartNew(() => DoProcessCommand(request));
        }

        public override Task<HandleEventsResponse> HandleEvents(HandleEventsRequest request, ServerCallContext context)
        {
            return Task<HandleEventsResponse>.Factory.StartNew(() => DoHandleEvents(request));
        }
    }
}