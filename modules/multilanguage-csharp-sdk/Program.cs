using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using System.Net;
using Microsoft.Extensions.Logging;

namespace Surge 
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        // Additional configuration is required to successfully run gRPC on macOS.
        // For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682
        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureLogging(logging =>
                {
                    // Enable Microsoft Grpc.Net logging
                    logging.AddFilter("Grpc", LogLevel.Debug);
                })
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    //ConfigureInsecureHttp2Listener(webBuilder);
                    webBuilder.UseStartup<Startup>();
                });

        private static void ConfigureInsecureHttp2Listener(IWebHostBuilder webBuilder)
        {
            // Makes Grpc.Core client with ChannelCredentials.Insecure work 
            webBuilder.ConfigureKestrel(options =>
            {
                // This endpoint will use HTTP/2 and HTTPS on port 5001.
                options.Listen(IPAddress.Loopback, 5001, listenOptions =>
                {
                    listenOptions.Protocols = HttpProtocols.Http2;
                });
            });
        }
    }
}
