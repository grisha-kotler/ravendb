﻿using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Microsoft.AspNetCore.Http;

namespace Raven.Server.Web.TEMP_REMOVE_ME
{
    public class MakeStudioWorkForNowHandler : RequestHandler
    {
        [RavenAction("/databases/*/configuration/document$", "GET")]
        public Task FakeResponseForConfigurationDocument()
        {
            HttpContext.Response.StatusCode = 404;
            return Task.CompletedTask;
        }
    }
}