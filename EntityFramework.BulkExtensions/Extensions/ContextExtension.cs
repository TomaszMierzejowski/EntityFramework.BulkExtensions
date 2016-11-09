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
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        private static DataTable CreateDataTable<T>(this DbContext context) where T : class
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

            table.TableName = nameof(T);
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
            var tb = context.CreateDataTable<T>();
            var tableColumns = context.GetTableColumns<T>().ToList();
            var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

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

        internal static DbContextTransaction InternalTransaction(this DbContext context)
        {
            DbContextTransaction transaction = null;
            if (context.Database.CurrentTransaction == null)
            {
                transaction = context.Database.BeginTransaction();
            }
            return transaction;
        }
    }
}