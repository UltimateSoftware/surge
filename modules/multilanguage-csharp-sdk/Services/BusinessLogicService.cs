using System;
using System.Threading.Tasks;
using com.ukg.surge.multilanguage.protobuf;
using Grpc.Core;

namespace Surge
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class BusinessLogicServiceImpl: BusinessLogicService.BusinessLogicServiceBase
    {
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
            throw new NotImplementedException();
        }

        public override Task<HandleEventsResponse> HandleEvents(HandleEventsRequest request, ServerCallContext context)
        {
            throw new NotImplementedException();
        }
    }
}
