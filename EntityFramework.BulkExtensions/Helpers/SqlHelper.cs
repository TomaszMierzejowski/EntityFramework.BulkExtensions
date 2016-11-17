using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using EntityFramework.BulkExtensions.Extensions;
using EntityFramework.MappingAPI;
using EntityFramework.MappingAPI.Extensions;

namespace EntityFramework.BulkExtensions.Helpers
{
    /// <summary>
    /// </summary>
    internal static class SqlHelper
    {
        private const int RandomLength = 6;

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="context"></param>
        /// <returns></returns>
        internal static string RandomTableName<TEntity>(this DbContext context)
        {
            var schema = context.Db<TEntity>().Schema;
            return $"[{schema}].[_tmp{Guid.NewGuid().ToString().Substring(0, RandomLength)}]";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="context"></param>
        /// <param name="tableName"></param>
        /// <param name="primaryKeysOnly"></param>
        /// <returns></returns>
        internal static string BuildCreateTempTable<TEntity>(this DbContext context, string tableName, bool primaryKeysOnly = false) where TEntity : class
        {
            var columns = context.GetTableColumns<TEntity>();
            columns = primaryKeysOnly ? columns.Where(map => map.IsPk) : columns;
            var command = new StringBuilder();

            command.Append($"CREATE TABLE {tableName}(");

            var primitiveTypes = context.GetPrimitiveType<TEntity>();
            var paramList = columns
                .Select(column => $"[{column.ColumnName}] {column.GetSchemaType(primitiveTypes[column.ColumnName])}")
                .ToList();
            var paramListConcatenated = string.Join(", ", paramList);

            command.Append(paramListConcatenated);
            command.Append(");");

            return command.ToString();
        }

        /// <summary>
        /// </summary>
        /// <param name="context"></param>
        /// <param name="dataTable"></param>
        /// <param name="tableName"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        internal static void BulkInsertToTable(this Database context, DataTable dataTable, string tableName,
            SqlBulkCopyOptions sqlBulkCopyOptions)
        {
            using (
                var bulkcopy = new SqlBulkCopy((SqlConnection)context.Connection, sqlBulkCopyOptions,
                    (SqlTransaction)context.CurrentTransaction.UnderlyingTransaction))
            {
                bulkcopy.DestinationTableName = tableName;
                bulkcopy.BulkCopyTimeout = context.Connection.ConnectionTimeout;
                bulkcopy.WriteToServer(dataTable);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        internal static string GetDropTableCommand(string tableName)
        {
            return $"DROP TABLE {tableName};";
        }

        /// <summary>
        /// </summary>
        /// <param name="context"></param>
        /// <typeparam name="TEntity"></typeparam>
        /// <returns></returns>
        internal static string BuildUpdateSet<TEntity>(this DbContext context) where TEntity : class
        {
            var command = new StringBuilder();
            var parameters = new List<string>();
            var tableColumns = context.GetTableColumns<TEntity>();

            command.Append("SET ");

            foreach (var column in tableColumns)
            {
                if (column.IsPk) continue;

                parameters.Add(
                    $"[{Constants.Target}].[{column.ColumnName}] = [{Constants.Source}].[{column.ColumnName}]");
            }

            command.Append(string.Join(", ", parameters) + " ");

            return command.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="context"></param>
        /// <returns></returns>
        internal static string PrimaryKeysComparator<TEntity>(this DbContext context) where TEntity : class
        {
            var updateOn = context.GetTablePKs<TEntity>().ToList();
            var command = new StringBuilder();

            command.Append($"ON [{Constants.Target}].[{updateOn.First()}] = [{Constants.Source}].[{updateOn.First()}] ");

            if (updateOn.Count > 1)
                foreach (var key in updateOn.Skip(1))
                    command.Append($"AND [{Constants.Target}].[{key}] = [{Constants.Source}].[{key}]");

            return command.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="context"></param>
        /// <param name="tmpOutputTableName"></param>
        /// <param name="tmpTableName"></param>
        /// <param name="identityColumn"></param>
        /// <returns></returns>
        internal static string GetInsertIntoStagingTableCmd<TEntity>(this DbContext context, string tmpOutputTableName,
            string tmpTableName, string identityColumn) where TEntity : class
        {
            var fullTableName = context.GetTableName<TEntity>();
            var columns = context.GetTableColumns<TEntity>().Select(map => map.ColumnName).ToList();

            var comm = GetOutputCreateTableCmd(tmpOutputTableName, identityColumn)
                       + BuildInsertIntoSet(columns, identityColumn, fullTableName)
                       + $"OUTPUT INSERTED.{identityColumn} INTO "
                       + tmpOutputTableName + $"([{identityColumn}]) "
                       + BuildSelectSet(columns, identityColumn)
                       + $" FROM {tmpTableName} AS Source; "
                       + GetDropTableCommand(tmpTableName);

            return comm;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="context"></param>
        /// <param name="tmpOutputTableName"></param>
        /// <param name="identityColumn"></param>
        /// <param name="items"></param>
        internal static void LoadFromTmpOutputTable<TEntity>(this Database context, string tmpOutputTableName,
            string identityColumn, IList<TEntity> items)
        {
            var command = $"SELECT {identityColumn} FROM {tmpOutputTableName} ORDER BY {identityColumn};";
            var identities = context.SqlQuery<int>(command);
            var counter = 0;

            foreach (var result in identities)
            {
                var property = items[counter].GetType().GetProperty(identityColumn);

                if (property.CanWrite)
                    property.SetValue(items[counter], result, null);

                else
                    throw new Exception();

                counter++;
            }

            command = GetDropTableCommand(tmpOutputTableName);
            context.ExecuteSqlCommand(command);
        }

        private static string BuildSelectSet(IEnumerable<string> columns, string identityColumn)
        {
            var command = new StringBuilder();
            var selectColumns = new List<string>();

            command.Append("SELECT ");

            foreach (var column in columns.ToList())
            {
                if (((identityColumn == null) || (column == identityColumn)) && (identityColumn != null)) continue;
                selectColumns.Add($"[{Constants.Source}].[{column}]");
            }

            command.Append(string.Join(", ", selectColumns));

            return command.ToString();
        }

        private static string BuildInsertIntoSet(IEnumerable<string> columns, string identityColumn, string tableName)
        {
            var command = new StringBuilder();
            var insertColumns = new List<string>();

            command.Append("INSERT INTO ");
            command.Append(tableName);
            command.Append(" (");

            foreach (var column in columns)
                if (column != identityColumn)
                    insertColumns.Add($"[{column}]");

            command.Append(string.Join(", ", insertColumns));
            command.Append(") ");

            return command.ToString();
        }

        private static string GetOutputCreateTableCmd(string tmpTablename, string identityColumn)
        {
            return $"CREATE TABLE {tmpTablename}([{identityColumn}] int); ";
        }

        private static string GetSchemaType(this IPropertyMap column, string columnType)
        {
            switch (columnType)
            {
                case "varchar":
                case "nvarchar":
                case "char":
                case "binary":
                case "varbinary":
                case "nchar":
                    if (column.MaxLength != 0)
                        columnType = columnType + $"({column.MaxLength})";
                    break;
                case "decimal":
                case "numeric":
                    columnType = columnType + $"({column.Precision}, {column.Scale})";
                    break;
                case "datetime2":
                case "time":
                    //columnType = columnType + "(" + column. + ")";
                    break;
            }

            return columnType;
        }
    }
}