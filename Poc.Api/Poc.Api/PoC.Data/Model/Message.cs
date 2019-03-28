using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PoC.Data.Model
{
    public class Message
    {
        public Guid? id { get; set; }
        public string imei { get; set; }
        public decimal avgspeed { get; set; }
        public DateTime windowendtime { get; set; }
    }
}
