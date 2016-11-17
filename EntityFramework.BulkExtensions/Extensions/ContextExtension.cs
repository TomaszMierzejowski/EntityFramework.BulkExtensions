using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Linq;
using System.Reflection;

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
        /// <param name="entities"></param>
        /// <param name="primaryKeysOnly"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        internal static DataTable ToDataTable<TEntity>(this DbContext context, IEnumerable<TEntity> entities, bool primaryKeysOnly = false) where TEntity : class
        {
            var tb = context.CreateDataTable<TEntity>(primaryKeysOnly);
            var tableColumns = context.GetTableColumns<TEntity>().ToList();
            var props = typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            foreach (var item in entities)
            {
                var values = new List<object>();
                foreach (var column in tableColumns)
                {
                    var prop = props.SingleOrDefault(info => info.Name == column.PropertyName);
                    if (prop != null)
                        values.Add(prop.GetValue(item, null));
                }

                tb.Rows.Add(values.ToArray());
            }

            return tb;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        internal static DbContextTransaction InternalTransaction(this DbContext context)
        {
            DbContextTransaction transaction = null;
            if (context.Database.CurrentTransaction == null)
            {
                transaction = context.Database.BeginTransaction();
            }
            return transaction;
        }

        private static DataTable CreateDataTable<TEntity>(this DbContext context, bool primaryKeysOnly = false) where TEntity : class
        {
            var table = new DataTable();
            var columns = context.GetTableColumns<TEntity>();
            columns = primaryKeysOnly ? columns.Where(map => map.IsPk) : columns;
            foreach (var prop in columns)
            {
                table.Columns.Add(prop.ColumnName, Nullable.GetUnderlyingType(prop.Type) ?? prop.Type);
            }

            table.TableName = nameof(TEntity);
            return table;
        }
    }
}