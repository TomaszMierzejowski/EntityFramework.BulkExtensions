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
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal static string GetTableName<T>(this DbContext context) where T : class
        {
            var entityMap = context.Db<T>();
            return $"[{entityMap.Schema}].[{entityMap.TableName}]";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal static IEnumerable<string> GetTablePKs<T>(this DbContext context) where T : class
        {
            var entityMap = context.Db<T>();
            return entityMap.Pks.Select(map => map.ColumnName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal static IEnumerable<IPropertyMap> GetTableColumns<T>(this DbContext context) where T : class
        {
            var entityMap = context.Db<T>();
            return entityMap.Properties
                .Where(map => !map.IsNavigationProperty)
                .ToList();
        }

        internal static IDictionary<string, string> GetPrimitiveType<T>(this DbContext context) where T : class
        {
            var map = new Dictionary<string, string>();
            var entityMap = context.EntitySchema<T>().Members.ToList();

            foreach (var member in entityMap)
            {
                map.Add(member.Name, member.TypeUsage.EdmType.Name);
            }

            return map;
        }

        private static EntityType EntitySchema<T>(this IObjectContextAdapter context) where T : class
        {
            var items = context.ObjectContext.MetadataWorkspace
                .GetItems<EntityType>(DataSpace.SSpace);
            var name = typeof(T).Name;

            return items.SingleOrDefault(type => type.Name == name);
        }
    }
}