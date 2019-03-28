using PoC.Data.Model;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PoC.Data.Service
{
    public interface IMessageService
    {
        IEnumerable<Message> GetCassandraData(string imei);
    }
}
