using System;
using System.Threading.Tasks;
using com.ukg.surge.multilanguage.protobuf;
using Grpc.Core;
using LanguageExt;
using LanguageExt;
using static LanguageExt.Prelude;
using Tuple = System.Tuple;

namespace Surge
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class BusinessLogicServiceImpl<TS, TE, TC>: BusinessLogicService.BusinessLogicServiceBase
    {
        public BusinessLogicServiceImpl()
        {
            // empty constructor
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

        public override Task<ProcessCommandReply> ProcessCommand(ProcessCommandRequest request, ServerCallContext context)
        {
            // deserialize state
            // TODO: check null state
            var state = SerDeser.DeserializeState.Invoke(request.State.Payload.ToByteArray());
            var command = SerDeser.DeserializeCommand.Invoke(request.Command.Payload.ToByteArray());
            Option<TS> maybeState;
            maybeState = new Some<TS>(state);
            var t = Tuple.Create(maybeState, command);
            Either<string, Lst<TE>> result;
            result = CqrsModel.CommandHandler.Invoke(t);
            if (result.IsLeft)
            {
                var reply = new ProcessCommandReply();
                reply.IsSuccess = false;
                reply.RejectionMessage = result.LeftToSeq().Head;
                throw new NotImplementedException();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public override Task<HandleEventsResponse> HandleEvents(HandleEventsRequest request, ServerCallContext context)
        {
            throw new NotImplementedException();
        }
    }
}
