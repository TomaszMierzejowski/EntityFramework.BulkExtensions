using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Core.Objects;
using System.Text.RegularExpressions;

namespace EntityFramework.BulkExtensions.Metadata
{
    public class EntityMetadata<T> where T : class
    {
        private readonly EntityType _entityType;
        private readonly ObjectSet<T> _entitySet;

        public EntityMetadata(EntityType entityType, ObjectSet<T> set)
        {
            _entityType = entityType;
            _entitySet = set;
        }

        public string Table
        {
            get
            {
                var fullName = GetFullName();
                var regex = new Regex(@"\[.*\].\[(.*)\]");
                var tableName = regex.Match(fullName).Groups[1];
                return tableName.Value;
            }
        }

        public string Schema
        {
            get
            {
                var fullName = GetFullName();
                var regex = new Regex(@"\[.*\].\[(.*)\]");
                var tableName = regex.Match(fullName).Groups[1];
                return tableName.Value;
            }
        }

        private string GetFullName()
        {
            var completeNameRegex = new Regex("FROM (?<table>.*) AS");
            var match = completeNameRegex.Match(_entitySet.ToTraceString());
            return match.Groups["table"].Value;
        }
    }
}