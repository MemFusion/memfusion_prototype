using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace ConsoleApplication1
{
    public class Entity
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            Aggregate();
        }
        static void Aggregate()
        {
            var connectionString = "mongodb://localhost";
            var client = new MongoClient(connectionString);
            var server = client.GetServer();
            var database = server.GetDatabase("test");
            var coll = database.GetCollection<Entity>("zips");

            var pipeline = new BsonArray
            {
                new BsonDocument
                {
                    { "$group",
                        new BsonDocument
                        {
                            { "_id", new BsonDocument
                                {
                                    {
                                        "state","$state"
                                    }
                                } 

                            },
                            {
                                "TotalPop", new BsonDocument
                                {
                                    {
                                        "$sum", "$pop"
                                    }
                                }
                            }
                        }
                    }
            }
            };

            var command = new CommandDocument
            {
                { "aggregate", "zips" },
                { "pipeline", pipeline }
            };

            var result = database.RunCommand(command);

            Console.WriteLine("{0}", result);
        }
        static void Test1(string[] args)
        {
            var connectionString = "mongodb://localhost";
            var client = new MongoClient(connectionString);
            var server = client.GetServer();
            var database = server.GetDatabase("test");
            var collection = database.GetCollection<Entity>("entities");

            var entity = new Entity { Name = "Tom" };
            collection.Insert(entity);
            var id = entity.Id;

            var query = Query<Entity>.EQ(e => e.Id, id);
            entity = collection.FindOne(query);

            entity.Name = "Dick";
            collection.Save(entity);

            var update = Update<Entity>.Set(e => e.Name, "Harry");
            collection.Update(query, update);

            collection.Remove(query);
        }
    }
}
