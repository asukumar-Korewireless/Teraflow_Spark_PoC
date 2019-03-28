using PoC.Data.Model;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PoC.Data.Repository
{
    public interface ICassandraRepository
    {
        IEnumerable<T> Get<T>(string imei);
    }
}
