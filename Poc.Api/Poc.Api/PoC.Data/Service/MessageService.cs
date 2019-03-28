using Microsoft.AspNetCore.SignalR;
using Newtonsoft.Json;
using PoC.Data.Model;
using PoC.Data.Repository;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Tasks;

namespace PoC.Data.Service
{
    public class MessageService : IMessageService
    {
        ICassandraRepository _cassandraRepository;

        public MessageService(ICassandraRepository cassandraRepository)
        {
            _cassandraRepository = cassandraRepository;
            
        }
        public IEnumerable<Message> GetCassandraData(string imei=null)
        {
            return _cassandraRepository.Get<Message>(imei);
        }
    }
}
