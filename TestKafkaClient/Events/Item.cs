using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Events
{
    public class Item
    {

        public string Name { get; set; }

        public double Price { get; set; }

        public Item()
        {
            Name = string.Empty;
        }

        public Item(string name, double price)
        {
            Name = name;
            Price = price;
        }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

    }
}
