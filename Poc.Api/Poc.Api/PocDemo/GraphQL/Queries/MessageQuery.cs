using GraphQL.Types;
using PoC.Api.GraphQL.Types;
using PoC.Data.Service;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PoC.Api.GraphQL.Queries
{
    public class MessageQuery : ObjectGraphType<object>
    {
        public MessageQuery(IMessageService messageService)
        {
            Name = "MessageQuery";
            Field<ListGraphType<MessageType>>(
                name: "get_data",
                description: "Get data from cassandra table",
                arguments: new QueryArguments { new QueryArgument<StringGraphType> { Name = "imei" } },
                resolve: context => {
                    var imei = context.GetArgument<string>("imei");
                    return messageService.GetCassandraData(imei);
                });
        }
    }
}
