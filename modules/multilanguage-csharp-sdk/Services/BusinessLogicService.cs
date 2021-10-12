using System;
using System.Threading.Tasks;
using com.ukg.surge.multilanguage.protobuf;
using Grpc.Core;

namespace Surge
{
    public class BusinessLogicServiceImpl: BusinessLogicService.BusinessLogicServiceBase
    {
        public override Task<HealthCheckReply> HealthCheck(HealthCheckRequest request, ServerCallContext context)
        {
            throw new NotImplementedException();
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