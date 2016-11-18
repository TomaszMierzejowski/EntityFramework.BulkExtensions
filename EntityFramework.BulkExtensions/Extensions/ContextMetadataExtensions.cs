using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure;
using System.Linq;
using EntityFramework.MappingAPI;
using EntityFramework.MappingAPI.Extensions;

namespace EntityFramework.BulkExtensions.Extensions
{
    public static class ContextMetadataExtensions
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        internal static string GetTableName<TEntity>(this DbContext context) where TEntity : class
        {
            var entityMap = context.Db<TEntity>();
            return $"[{entityMap.Schema}].[{entityMap.TableName}]";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        internal static IEnumerable<IPropertyMap> GetTablePKs<TEntity>(this DbContext context) where TEntity : class
        {
            var entityMap = context.Db<TEntity>();
            return entityMap.Pks;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        internal static IEnumerable<IPropertyMap> GetTableColumns<TEntity>(this DbContext context) where TEntity : class
        {
            var entityMap = context.Db<TEntity>();
            return entityMap.Properties
                .Where(map => !map.IsNavigationProperty)
                .ToList();
        }

        internal static IDictionary<string, string> GetPrimitiveType<TEntity>(this DbContext context) where TEntity : class
        {
            var map = new Dictionary<string, string>();
            var entityMap = context.EntitySchema<TEntity>().Members.ToList();

            foreach (var member in entityMap)
            {
                map.Add(member.Name, member.TypeUsage.EdmType.Name);
            }

            return map;
        }

        private static EntityType EntitySchema<TEntity>(this IObjectContextAdapter context) where TEntity : class
        {
            var items = context.ObjectContext.MetadataWorkspace
                .GetItems<EntityType>(DataSpace.SSpace);
            var name = typeof(TEntity).Name;

            return items.SingleOrDefault(type => type.Name == name);
        }

        public static bool Exists<TEntity>(this IObjectContextAdapter context) where TEntity : class
        {
            var entityName = typeof(TEntity).Name;
            var workspace = context.ObjectContext.MetadataWorkspace;
            return workspace.GetItems<EntityType>(DataSpace.CSpace).Any(e => e.Name == entityName);
        }
    }
}