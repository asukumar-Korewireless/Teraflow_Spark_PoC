using GraphQL.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PoC.Api.GraphQL
{
    public class Helper
    {
        public static Dictionary<System.Type, System.Type> Map = new Dictionary<System.Type, System.Type>
        {
            {typeof(string), typeof(StringGraphType)},
            {typeof(bool), typeof(BooleanGraphType)},
            {typeof(int), typeof(IntGraphType)},
            {typeof(int?), typeof(IntGraphType)},
            {typeof(float), typeof(FloatGraphType)},
            {typeof(decimal), typeof(DecimalGraphType)},
            {typeof(Guid), typeof(IdGraphType)},
            {typeof(Guid?), typeof(IdGraphType)},
            {typeof(double), typeof(FloatGraphType)},
            {typeof(DateTime), typeof(DateTimeGraphType)}

        };
    }
}
