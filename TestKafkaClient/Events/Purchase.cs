using Newtonsoft.Json;

namespace Events
{
    public class Purchase
    {

        public const string TOPIC_NAME = "purchases";

        public string UserId { get; set; }

        public List<string> Items { get; set; } 

        public double Amount { get; set; }

        public Purchase()
        {
            UserId = string.Empty;
            Items = new List<string>();
        }

        public Purchase(string userId, List<string> items, double amount)
        {
            UserId = userId;
            Items = items;
            Amount = amount;
        }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}