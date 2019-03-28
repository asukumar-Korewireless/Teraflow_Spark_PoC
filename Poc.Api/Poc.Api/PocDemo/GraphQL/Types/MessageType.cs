using GraphQL.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PoC.Data.Model;

namespace PoC.Api.GraphQL.Types
{
    public class MessageType : ObjectGraphType
    {
        public MessageType()
        {
            Name = "Message";
            foreach (var prop in typeof(Message).GetProperties())
            {
                Field(prop.Name, x => prop.GetValue(x), type: Helper.Map[prop.PropertyType]);
            }
        }
    }
}
