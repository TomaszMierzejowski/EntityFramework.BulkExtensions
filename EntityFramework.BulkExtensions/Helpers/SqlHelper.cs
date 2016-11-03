using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using EntityFramework.BulkExtensions.Extensions;
using EntityFramework.MappingAPI;

namespace EntityFramework.BulkExtensions.Helpers
{
    /// <summary>
    /// 
    /// </summary>
    internal static class SqlHelper
    {
        private const int RandomLength = 6;
        internal static string RandomTableName()
        {
            return $"#tmp{Guid.NewGuid().ToString().Substring(0, RandomLength)}";
        }

        internal static string BuildCreateTempTable<T>(this DbContext context, string tableName, ColumnDirection outputIdentity) where T : class
        {
            var columns = context.GetTableColumns<T>();
            var command = new StringBuilder();

            command.Append($"CREATE TABLE {tableName}(");

            var paramList = new List<string>();
            var primitiveTypes = context.GetPrimitiveType<T>();

            foreach (var column in columns)
            {
                paramList.Add($"[{column.ColumnName}] {column.GetSchemaType(primitiveTypes[column.ColumnName])}");
            }

            var paramListConcatenated = string.Join(", ", paramList);

            command.Append(paramListConcatenated);

            if (outputIdentity == ColumnDirection.InputOutput)
            {
                command.Append($", [{Constants.InternalId}] int");
            }
            command.Append(");");

            return command.ToString();
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
                    {
                        columnType = columnType + "(" + column.MaxLength + ")";
                    }
                    break;
                case "decimal":
                case "numeric":
                    columnType = columnType + "(" + column.Precision + ", " + column.Scale + ")";
                    break;
                case "datetime2":
                case "time":
                    //columnType = columnType + "(" + column. + ")";
                    break;
            }

            return columnType;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="dt"></param>
        /// <param name="tableName"></param>
        /// <param name="sqlBulkCopyOptions"></param>
        internal static void InsertToTmpTable(this Database context, DataTable dt, string tableName, SqlBulkCopyOptions sqlBulkCopyOptions)
        {
            using (var bulkcopy = new SqlBulkCopy((SqlConnection)context.Connection, sqlBulkCopyOptions, (SqlTransaction)context.CurrentTransaction.UnderlyingTransaction))
            {
                bulkcopy.DestinationTableName = tableName;
                bulkcopy.BulkCopyTimeout = context.Connection.ConnectionTimeout;
                bulkcopy.WriteToServer(dt);
            }
        }

        /// <summary>
        /// Advanced Settings for SQLBulkCopy class. 
        /// </summary>
        /// <param name="bulkcopy"></param>
        /// <param name="bulkCopyEnableStreaming"></param>
        /// <param name="bulkCopyBatchSize"></param>
        /// <param name="bulkCopyNotifyAfter"></param>
        /// <param name="bulkCopyTimeout"></param>
        /// <param name="bulkCopyDelegates"></param>
        private static void SetSqlBulkCopySettings(SqlBulkCopy bulkcopy, bool bulkCopyEnableStreaming, int? bulkCopyBatchSize,
            int? bulkCopyNotifyAfter, int bulkCopyTimeout, IEnumerable<SqlRowsCopiedEventHandler> bulkCopyDelegates)
        {
            bulkcopy.EnableStreaming = bulkCopyEnableStreaming;

            if (bulkCopyBatchSize.HasValue)
            {
                bulkcopy.BatchSize = bulkCopyBatchSize.Value;
            }

            if (bulkCopyNotifyAfter.HasValue)
            {
                bulkcopy.NotifyAfter = bulkCopyNotifyAfter.Value;
                bulkCopyDelegates?.ToList().ForEach(x => bulkcopy.SqlRowsCopied += x);
            }

            bulkcopy.BulkCopyTimeout = bulkCopyTimeout;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        internal static string BuildUpdateSet<T>(this DbContext context) where T : class
        {
            var command = new StringBuilder();
            var paramsSeparated = new List<string>();
            var tableColumns = context.GetTableColumns<T>(true);

            command.Append("SET ");

            foreach (var column in tableColumns)
            {
                //if (column.IsPk) continue;

                paramsSeparated.Add("[" + Constants.TargetAlias + "]" + "." + "[" + column.ColumnName + "]" + " = " + "[" + Constants.SourceAlias + "]" + "."
                                    + "[" + column.ColumnName + "]");
            }

            command.Append(string.Join(", ", paramsSeparated) + " ");

            return command.ToString();
        }

        internal static string BuildMatchPKs<T>(this DbContext context) where T : class
        {
            var updateOn = context.GetTablePKs<T>().ToList();
            var command = new StringBuilder();

            command.Append("ON " + "[" + Constants.TargetAlias + "]" + "." + "[" + updateOn[0] + "]" + " = " + "[" + Constants.SourceAlias + "]" + "."
                + "[" + updateOn[0] + "]" + " ");

            if (updateOn.Count > 1)
            {
                // Start from index 1 to just append "AND" conditions
                for (var i = 1; i < updateOn.Count; i++)
                {
                    command.Append("AND " + "[" + Constants.TargetAlias + "]" + "." + "[" + updateOn[i] + "]" + " = " + "[" +
                        Constants.SourceAlias + "]" + "." + "[" + updateOn[i] + "]" + " ");
                }
            }

            return command.ToString();
        }

        internal static string GetOutputIdentityCmd(string tableName, string identityColumn, ColumnDirection outputIdentity, OperationType operation)
        {
            var sb = new StringBuilder();
            if (outputIdentity != ColumnDirection.InputOutput)
            {
                return null;
            }
            if (operation == OperationType.Insert)
                sb.Append("OUTPUT INSERTED." + identityColumn + " INTO " + tableName + "(" + identityColumn + "); ");

            else if (operation == OperationType.InsertOrUpdate || operation == OperationType.Update)
                sb.Append("OUTPUT Source." + Constants.InternalId + ", INSERTED." + identityColumn + " INTO " + tableName
                    + "(" + Constants.InternalId + ", " + identityColumn + "); ");

            else if (operation == OperationType.Delete)
                sb.Append("OUTPUT Source." + Constants.InternalId + ", DELETED." + identityColumn + " INTO " + tableName
                    + "(" + Constants.InternalId + ", " + identityColumn + "); ");

            return sb.ToString();
        }

        internal static string GetOutputCreateTableCmd(ColumnDirection outputIdentity, string tmpTablename, OperationType operation, string identityColumn)
        {

            if (operation == OperationType.Insert)
                return (outputIdentity == ColumnDirection.InputOutput ? "CREATE TABLE " + tmpTablename + "(" + "[" + identityColumn + "] int); " : "");

            else if (operation == OperationType.InsertOrUpdate || operation == OperationType.Update || operation == OperationType.Delete)
                return (outputIdentity == ColumnDirection.InputOutput ? "CREATE TABLE " + tmpTablename + "("
                    + "[" + Constants.InternalId + "]" + " int, [" + identityColumn + "] int); " : "");

            return string.Empty;
        }

        internal static void LoadFromTmpOutputTable<T>(this Database context, string identityColumn, Dictionary<int, T> outputIdentityDic,
            OperationType operationType, IEnumerable<T> list)
        {
            if (operationType == OperationType.InsertOrUpdate
                || operationType == OperationType.Update
                || operationType == OperationType.Delete)
            {
                //var command = "SELECT " + Constants.InternalId + ", " + identityColumn + " FROM "
                //    + Constants.TempOutputTableName + ";";
                //var identities = context.SqlQuery<int>(command);

                //foreach (var result in identities)
                //{
                //    T item;

                //    if (outputIdentityDic.TryGetValue((int)reader[0], out item))
                //    {
                //        PropertyInfo p = item.GetType().GetProperty(identityColumn);

                //        if (p.CanWrite)
                //            p.SetValue(item, reader[1], null);

                //        else
                //            throw new Exception();
                //    }

                //}

                ////command = GetDropTmpTableCmd();
                //context.ExecuteSqlCommand(command);
            }

            if (operationType == OperationType.Insert)
            {
                var command = "SELECT " + identityColumn + " FROM " + Constants.TempOutputTableName + ";";
                var identities = context.SqlQuery<int>(command);

                var items = list.ToList();
                var counter = 0;

                foreach (var result in identities)
                {
                    var p = items[counter].GetType().GetProperty(identityColumn);

                    if (p.CanWrite)
                        p.SetValue(items[counter], result, null);

                    else
                        throw new Exception();

                    counter++;
                }

                //command = GetDropTmpTableCmd();
                context.ExecuteSqlCommand(command);
            }
        }
    }
}