using System.Data.Entity;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure;
using System.Linq;

namespace EntityFramework.BulkExtensions.Metadata
{
    public static class ContextMetadataExtension
    {
        public static EntityMetadata<T> Metadata<T>(this DbContext context) where T : class
        {
            return context.GetEntityMetadata<T>();
        }

        private static EntityMetadata<T> GetEntityMetadata<T>(this IObjectContextAdapter context) where T : class
        {
            var items = context.ObjectContext.MetadataWorkspace
                .GetItems<EntityType>(DataSpace.SSpace);
            var name = typeof(T).Name;
            var set = context.ObjectContext.CreateObjectSet<T>();

            return new EntityMetadata<T>(items.SingleOrDefault(type => type.Name == name), set);
        }
    }
}