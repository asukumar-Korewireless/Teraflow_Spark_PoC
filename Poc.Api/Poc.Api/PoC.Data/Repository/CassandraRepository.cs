using Cassandra;
using Cassandra.Mapping;
using PoC.Data.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PoC.Data.Repository
{
    public class CassandraRepository : ICassandraRepository
    {
        private readonly string END_POINT = "127.0.0.1";
        private readonly string KEY_SPACE = "koreone";
        private readonly string TABLE_NAME = "sample";

        private Cluster cluster;
        private ISession session;
        private IMapper mapper;

        public CassandraRepository()
        {
            cluster = Cluster.Builder().AddContactPoint(END_POINT).WithPort(9042).Build();
            session = cluster.Connect(KEY_SPACE);
            mapper = new Mapper(session);
        }

        public IEnumerable<T> Get<T>(string imei)
        {
            var query = $"select * from {KEY_SPACE}.{TABLE_NAME}";
            if (imei != null)
                query += $" WHERE imei='{imei}';";
            IEnumerable<T> result = mapper.Fetch<T>(query);
            return result;
        }

    }
}
