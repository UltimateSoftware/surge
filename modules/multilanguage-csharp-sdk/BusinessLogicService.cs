using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using com.ukg.surge.multilanguage.protobuf;
using Google.Protobuf;
using Grpc.Core;
using LanguageExt;

namespace Surge
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class BusinessLogicServiceImpl<TS, TE, TC>: BusinessLogicService.BusinessLogicServiceBase
    {
        public BusinessLogicServiceImpl(CqrsModel<TS, TE, TC> cqrsModel, SerDeser<TS, TE, TC> serDeser)
        {
            CqrsModel = cqrsModel;
            SerDeser = serDeser;
        }

        public CqrsModel<TS, TE, TC> CqrsModel { get; set;  }
        
        public SerDeser<TS, TE, TC> SerDeser { get; set; }

        public override Task<HealthCheckReply> HealthCheck(HealthCheckRequest request, ServerCallContext context)
        {
            var reply = new HealthCheckReply
            {
                Status = HealthCheckReply.Types.Status.Up,
                ServiceName = "BusinessLogicService"
            };
            return Task<HealthCheckReply>.Factory.StartNew(() => reply);
        }

        private ProcessCommandReply doProcessCommand(ProcessCommandRequest request)
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
            TC command = SerDeser.DeserializeCommand.Invoke(request.Command.Payload.ToByteArray());

            Either<string, Lst<TE>> result = CqrsModel.CommandHandler.Invoke(Tuple.Create(maybeState, command));

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
                    Lst<TE> events = result.RightToSeq().Head;
                    Option<TS>  zeroState = Option<TS>.None;
                    // calculate the new state by doing a left over the events 
                    Option<TS> newState = events.Fold(zeroState, (ses, e) =>
                        CqrsModel.EventHandler.Invoke(Tuple.Create(ses, e)));

                    State newStatePb = null;
                    if (newState.IsSome)
                    {
                        newStatePb = new State
                        {
                            AggregateId = request.AggregateId,
                            // Serialize the calculated state to protocol buffers
                            Payload = ByteString.CopyFrom(SerDeser.SerializeState.Invoke(newState.ToList().Head()))
                        };
                    }

                    var reply = new ProcessCommandReply
                    {
                        NewState = newStatePb,
                        AggregateId = request.AggregateId,
                        IsSuccess = true,
                        RejectionMessage = null,
                    };
                    return reply;
                }
            } 
        }

        public HandleEventsResponse doHandleEvents(HandleEventsRequest handleEventsRequest)
        {
            Option<TS> state = Option<TS>.None;
            if (!handleEventsRequest.State.IsNull())
            {
                // deserialize the state (from protocol buffers) if it's there
               state = Option<TS>.Some(SerDeser.DeserializeState.Invoke(handleEventsRequest.State.Payload.ToByteArray())); 
            }
            // deserialize all events (from protocol buffers)
            IEnumerable<TE> events =
                handleEventsRequest.Events.Map(e => SerDeser.DeserializeEvent(e.Payload.ToByteArray()));
            // calculate new state
            Option<TS> newState = events.Fold(state, (ses, e) => CqrsModel.EventHandler.Invoke(Tuple.Create(ses, e))); 
            
            // serialize new state to protocol buffers
            State resultingStatePb = null;
            if (newState.IsSome)
            {
                resultingStatePb = new State
                {
                    AggregateId = handleEventsRequest.AggregateId,
                    Payload = ByteString.CopyFrom(SerDeser.SerializeState.Invoke(newState.ToList().Head()))
                };
            }
            
            var result = new HandleEventsResponse
            {
                AggregateId = handleEventsRequest.AggregateId,
                State = resultingStatePb
            };
            return result;
        }

        public override Task<ProcessCommandReply> ProcessCommand(ProcessCommandRequest request, ServerCallContext context)
        {
            return Task<ProcessCommandReply>.Factory.StartNew(() => doProcessCommand(request));
        }

        public override Task<HandleEventsResponse> HandleEvents(HandleEventsRequest request, ServerCallContext context)
        {
            return Task<HandleEventsResponse>.Factory.StartNew(() => doHandleEvents(request));
        }
    }
}
