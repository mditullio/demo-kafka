using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Events
{
    public class PurchaseSerializer : ISerializer<Purchase>, IDeserializer<Purchase>
    {
        public Purchase? Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
            {
                return null;
            }

            return JsonConvert.DeserializeObject<Purchase>(Deserializers.Utf8.Deserialize(data, isNull, context));
        }

        public byte[] Serialize(Purchase data, SerializationContext context)
        {
            return Serializers.Utf8.Serialize(JsonConvert.SerializeObject(data), context);
        }
    }
}
