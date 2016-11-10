using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using EntityFramework.BulkExtensions.Extensions;
using EntityFramework.BulkExtensions.Helpers;
using EntityFramework.BulkExtensions.Operations;
using SqlBulkTools;

namespace EntityFramework.BulkExtensions.BulkOperations
{
    /// <summary>
    /// 
    /// </summary>
    public class BulkUpdate : IBulkOperation
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="collection"></param>
        /// <param name="identity"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        int IBulkOperation.CommitTransaction<T>(DbContext context, IEnumerable<T> collection, Identity identity)
        {
            var tmpTableName = context.RandomTableName<T>();
            var entityList = collection.ToList();
            var database = context.Database;
            var affectedRows = 0;
            if (!entityList.Any())
            {
                return affectedRows;
            }

            //Creates inner transaction if the context doens't have one.
            var transaction = context.InternalTransaction();
            try
            {
                //Cconvert entity collection into a DataTable
                var dataTable = context.ToDataTable(entityList);
                //Create temporary table.
                var command = context.BuildCreateTempTable<T>(tmpTableName);
                database.ExecuteSqlCommand(command);

                //Bulk inset data to temporary temporary table.
                database.BulkInsertToTable(dataTable, tmpTableName, SqlBulkCopyOptions.Default);

                //Copy data from temporary table to destination table.
                command = $"MERGE INTO {context.GetTableName<T>()} WITH (HOLDLOCK) AS Target USING {tmpTableName} AS Source " +
                          $"{context.PrimaryKeysComparator<T>()} WHEN MATCHED THEN UPDATE {context.BuildUpdateSet<T>()}; " +
                          SqlHelper.GetDropTableCommand(tmpTableName);

                affectedRows = database.ExecuteSqlCommand(command);

                //Commit if internal transaction exists.
                transaction?.Commit();
                return affectedRows;
            }
            catch (Exception)
            {
                //Rollback if internal transaction exists.
                transaction?.Rollback();
                throw;
            }
        }

        public Task<int> CommitTransactionAsync<T>(DbContext context, IEnumerable<T> collection)
        {
            throw new NotImplementedException();
        }
    }
}
