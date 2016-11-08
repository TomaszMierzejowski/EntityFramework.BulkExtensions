using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.Entity.Core.Metadata.Edm;
using System.Data.Entity.Infrastructure;
using System.Linq;
using System.Reflection;
using EntityFramework.MappingAPI;
using EntityFramework.MappingAPI.Extensions;

namespace EntityFramework.BulkExtensions.Extensions
{
    /// <summary>
    /// </summary>
    internal static class ContextExtension
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="tableName"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        private static DataTable CreateDataTable<T>(this DbContext context, string tableName) where T : class
        {
            var table = new DataTable();
            foreach (var prop in context.GetTableColumns<T>())
            {
                if (Nullable.GetUnderlyingType(prop.Type) != null)
                {
                    table.Columns.Add(prop.ColumnName, Nullable.GetUnderlyingType(prop.Type));
                }
                else
                {
                    table.Columns.Add(prop.ColumnName, prop.Type);
                }
            }

            table.TableName = tableName;
            return table;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="entities"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal static DataTable ToDataTable<T>(this DbContext context, IEnumerable<T> entities) where T : class
        {
            var tb = context.CreateDataTable<T>(nameof(T));
            var tableColumns = context.GetTableColumns<T>().ToList();
            var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var item in entities)
            {
                var values = new List<object>();
                foreach (var column in tableColumns)
                {
                    var prop = props.SingleOrDefault(info => info.Name == column.ColumnName);
                    if (prop != null)
                        values.Add(prop.GetValue(item, null));
                }

                tb.Rows.Add(values.ToArray());
            }

            return tb;
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