using GraphQL;
using GraphQL.Types;
using PoC.Api;
using PoC.Api.GraphQL;
using PoC.Api.GraphQL.Queries;
using PoC.Data.Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Poc.Api
{
    public class PoCSchema : Schema
    {
        public PoCSchema(IMessageService messageService)
        {
            Query = new MessageQuery(messageService);
        }
    }
}
